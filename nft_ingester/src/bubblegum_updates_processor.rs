use crate::db_v2::Task;
use crate::error::IngesterError;
use crate::flatbuffer_mapper::FlatbufferMapper;
use blockbuster::programs::bubblegum::{BubblegumInstruction, Payload};
use blockbuster::{
    instruction::{order_instructions, InstructionBundle, IxPair},
    program_handler::ProgramParser,
    programs::{bubblegum::BubblegumParser, ProgramParseResult},
};
use chrono::Utc;
use entities::enums::{
    ChainMutability, OwnerType, RoyaltyTargetType, SpecificationAssetClass, TokenStandard,
    UseMethod,
};
use entities::models::{BufferedTransaction, Updated};
use entities::models::{ChainDataV1, Creator, Uses};
use log::{debug, error};
use metrics_utils::IngesterMetricsConfig;
use mpl_bubblegum::types::LeafSchema;
use mpl_bubblegum::InstructionName;
use num_traits::FromPrimitive;
use plerkle_serialization::{Pubkey as FBPubkey, TransactionInfo};
use rocks_db::asset::AssetOwner;
use rocks_db::asset::{
    AssetAuthority, AssetCollection, AssetDynamicDetails, AssetLeaf, AssetStaticDetails,
};
use serde_json::json;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Instant;

pub const BUFFER_PROCESSING_COUNTER: i32 = 10;

#[derive(Clone)]
pub struct BubblegumTxProcessor {
    pub transaction_parser: Arc<FlatbufferMapper>,
    pub instruction_parser: Arc<BubblegumParser>,

    pub rocks_client: Arc<rocks_db::Storage>,

    pub json_tasks: Arc<Mutex<VecDeque<Task>>>,
    pub metrics: Arc<IngesterMetricsConfig>,
}

impl BubblegumTxProcessor {
    pub fn new(
        rocks_client: Arc<rocks_db::Storage>,
        metrics: Arc<IngesterMetricsConfig>,
        json_tasks: Arc<Mutex<VecDeque<Task>>>,
    ) -> Self {
        let instruction_parser = Arc::new(BubblegumParser {});

        let transaction_parser = Arc::new(FlatbufferMapper {});

        BubblegumTxProcessor {
            transaction_parser,
            instruction_parser,
            rocks_client,
            json_tasks,
            metrics,
        }
    }

    pub fn break_transaction<'i>(
        &self,
        tx: &'i TransactionInfo<'i>,
    ) -> VecDeque<(IxPair<'i>, Option<Vec<IxPair<'i>>>)> {
        let mut ref_set: HashSet<&[u8]> = HashSet::new();
        let k = self.instruction_parser.key();
        ref_set.insert(k.as_ref());
        order_instructions(ref_set, tx)
    }

    pub async fn process_transaction(
        &self,
        data: BufferedTransaction,
    ) -> Result<(), IngesterError> {
        let seen_at = Utc::now();

        let mut transaction_info_bytes = data.transaction.clone();

        if data.map_flatbuffer {
            let tx_update =
                    utils::flatbuffer::transaction_info_generated::transaction_info::root_as_transaction_info(
                        &data.transaction,
                    ).unwrap();
            transaction_info_bytes = self
                .transaction_parser
                .map_tx_fb_bytes(tx_update, seen_at)
                .unwrap();
        }
        let transaction_info =
            plerkle_serialization::root_as_transaction_info(transaction_info_bytes.as_slice())
                .unwrap();

        self.handle_transaction(transaction_info).await
    }

    fn instruction_name_to_string(&self, ix: &InstructionName) -> &'static str {
        match ix {
            InstructionName::Unknown => "Unknown",
            InstructionName::MintV1 => "MintV1",
            InstructionName::MintToCollectionV1 => "MintToCollectionV1",
            InstructionName::Redeem => "Redeem",
            InstructionName::CancelRedeem => "CancelRedeem",
            InstructionName::Transfer => "Transfer",
            InstructionName::Delegate => "Delegate",
            InstructionName::DecompressV1 => "DecompressV1",
            InstructionName::Compress => "Compress",
            InstructionName::Burn => "Burn",
            InstructionName::CreateTree => "CreateTree",
            InstructionName::VerifyCreator => "VerifyCreator",
            InstructionName::UnverifyCreator => "UnverifyCreator",
            InstructionName::VerifyCollection => "VerifyCollection",
            InstructionName::UnverifyCollection => "UnverifyCollection",
            InstructionName::SetAndVerifyCollection => "SetAndVerifyCollection",
            InstructionName::SetDecompressibleState => "SetDecompressibleState",
            InstructionName::UpdateMetadata => "UpdateMetadata",
        }
    }

    // PDA lookup requires an 8-byte array.
    fn u32_to_u8_array(&self, value: u32) -> [u8; 8] {
        let bytes: [u8; 4] = value.to_le_bytes();
        let mut result: [u8; 8] = [0; 8];
        result[..4].copy_from_slice(&bytes);
        result
    }

    pub async fn handle_transaction<'a>(
        &self,
        tx: TransactionInfo<'a>,
    ) -> Result<(), IngesterError> {
        let sig: Option<&str> = tx.signature();

        let instructions = self.break_transaction(&tx);

        let accounts = tx.account_keys().unwrap_or_default();

        let slot = tx.slot();

        let txn_id = tx.signature().unwrap_or("");

        let mut keys: Vec<FBPubkey> = Vec::with_capacity(accounts.len());
        for k in accounts.into_iter() {
            keys.push(*k);
        }

        let mut not_impl = 0;
        let ixlen = instructions.len();

        for (outer_ix, inner_ix) in instructions {
            let (program, instruction) = outer_ix;
            let ix_accounts = instruction.accounts().unwrap().iter().collect::<Vec<_>>();
            let ix_account_len = ix_accounts.len();
            let max = ix_accounts.iter().max().copied().unwrap_or(0) as usize;

            if keys.len() < max {
                return Err(IngesterError::DeserializationError(
                    "Missing Accounts in Serialized Ixn/Txn".to_string(),
                ));
            }

            let ix_accounts =
                ix_accounts
                    .iter()
                    .fold(Vec::with_capacity(ix_account_len), |mut acc, a| {
                        if let Some(key) = keys.get(*a as usize) {
                            acc.push(*key);
                        }
                        acc
                    });

            let ix = InstructionBundle {
                txn_id,
                program,
                instruction: Some(instruction),
                inner_ix,
                keys: ix_accounts.as_slice(),
                slot,
            };

            if ix.program.0 == mpl_bubblegum::programs::MPL_BUBBLEGUM_ID.to_bytes() {
                let result = self.instruction_parser.handle_instruction(&ix)?;

                let concrete = result.result_type();
                match concrete {
                    ProgramParseResult::Bubblegum(parsing_result) => {
                        self.metrics.inc_instructions(
                            self.instruction_name_to_string(&parsing_result.instruction),
                        );

                        self.handle_bubblegum_instruction(parsing_result, &ix)
                            .await
                            .map_err(|err| {
                                error!(
                                    "Failed to handle bubblegum instruction for txn {:?}: {:?}",
                                    sig, err
                                );

                                err
                            })?;
                    }
                    _ => {
                        not_impl += 1;
                    }
                };
            }
        }

        if not_impl == ixlen {
            return Err(IngesterError::NotImplemented);
        }

        self.metrics
            .set_last_processed_slot("transaction", slot as i64);

        Ok(())
    }

    pub async fn handle_bubblegum_instruction<'c>(
        &self,
        parsing_result: &'c BubblegumInstruction,
        bundle: &'c InstructionBundle<'c>,
    ) -> Result<(), IngesterError> {
        let ix_type = &parsing_result.instruction;

        let begin_processing = Instant::now();

        let ix_str = self.instruction_name_to_string(ix_type);
        debug!("BGUM instruction txn={:?}: {:?}", ix_str, bundle.txn_id);

        let mut processed = true;

        match ix_type {
            InstructionName::Transfer
            | InstructionName::CancelRedeem
            | InstructionName::Delegate => {
                self.update_owner(parsing_result, bundle).await?;
            }
            InstructionName::Burn => {
                self.burn(parsing_result, bundle).await?;
            }
            InstructionName::MintV1 | InstructionName::MintToCollectionV1 => {
                self.mint_v1(parsing_result, bundle).await?;
            }
            InstructionName::Redeem => {
                self.redeem(parsing_result, bundle).await?;
            }
            InstructionName::DecompressV1 => {
                self.decompress(parsing_result, bundle).await?;
            }
            InstructionName::VerifyCreator | InstructionName::UnverifyCreator => {
                self.creator_verification(parsing_result, bundle).await?;
            }
            InstructionName::VerifyCollection
            | InstructionName::UnverifyCollection
            | InstructionName::SetAndVerifyCollection => {
                self.collection_verification(parsing_result, bundle).await?;
            }
            InstructionName::UpdateMetadata => {
                self.update_metadata(parsing_result, bundle).await?;
            }
            _ => {
                debug!("Bubblegum: Not Implemented Instruction");
                processed = false;
            }
        }
        // save signature
        self.rocks_client
            .persist_signature(
                solana_sdk::pubkey::Pubkey::new_from_array(bundle.program.0),
                entities::models::SignatureWithSlot {
                    signature: Signature::from_str(bundle.txn_id)?,
                    slot: bundle.slot,
                },
            )
            .await
            .map_err(|e| IngesterError::TransactionNotProcessedError(e.to_string()))?;
        if processed {
            self.metrics.set_latency(
                "transactions_parser",
                begin_processing.elapsed().as_millis() as f64,
            );
        }
        Ok(())
    }

    pub async fn update_owner<'c>(
        &self,
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle<'c>,
    ) -> Result<(), IngesterError> {
        if let (Some(le), Some(cl)) = (&parsing_result.leaf_update, &parsing_result.tree_update) {
            self.rocks_client.save_changelog(cl, bundle.slot).await;

            match le.schema {
                LeafSchema::V1 {
                    id,
                    owner,
                    delegate,
                    ..
                } => {
                    if let Err(e) = self.rocks_client.save_tx_data_and_asset_updated(
                        id,
                        bundle.slot,
                        Some(AssetLeaf {
                            pubkey: id,
                            tree_id: cl.id,
                            leaf: Some(le.leaf_hash.to_vec()),
                            nonce: Some(cl.index as u64),
                            data_hash: Some(Hash::from(le.schema.data_hash())),
                            creator_hash: Some(Hash::from(le.schema.creator_hash())),
                            leaf_seq: Some(cl.seq),
                            slot_updated: bundle.slot,
                        }),
                        None,
                    ) {
                        error!("Error while saving tx_data for cNFT: {}", e);
                    };

                    if let Err(e) = self.rocks_client.asset_owner_data.merge(
                        id,
                        &AssetOwner {
                            pubkey: id,
                            owner: Updated::new(bundle.slot, Some(cl.seq), owner),
                            delegate: get_delegate(delegate, owner, bundle.slot, cl.seq),
                            owner_type: Updated::new(bundle.slot, Some(cl.seq), OwnerType::Single),
                            owner_delegate_seq: Some(Updated::new(
                                bundle.slot,
                                Some(cl.seq),
                                cl.seq,
                            )),
                        },
                    ) {
                        error!("Error while saving owner for cNFT: {}", e);
                    };
                }
            }

            return Ok(());
        }
        Err(IngesterError::ParsingError(
            "Ix not parsed correctly".to_string(),
        ))
    }

    pub async fn burn<'c>(
        &self,
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle<'c>,
    ) -> Result<(), IngesterError> {
        if let (Some(_le), Some(cl)) = (&parsing_result.leaf_update, &parsing_result.tree_update) {
            self.rocks_client.save_changelog(cl, bundle.slot).await;

            let (asset_id, _) = Pubkey::find_program_address(
                &[
                    "asset".as_bytes(),
                    cl.id.as_ref(),
                    self.u32_to_u8_array(cl.index).as_ref(),
                ],
                &mpl_bubblegum::ID,
            );

            if let Err(e) = self.rocks_client.save_tx_data_and_asset_updated(
                asset_id,
                bundle.slot,
                None,
                Some(AssetDynamicDetails {
                    pubkey: asset_id,
                    supply: Some(Updated::new(bundle.slot, Some(cl.seq), 0)),
                    is_burnt: Updated::new(bundle.slot, Some(cl.seq), true),
                    seq: Some(Updated::new(bundle.slot, Some(cl.seq), cl.seq)),
                    is_compressed: Updated::new(bundle.slot, Some(cl.seq), true),
                    ..Default::default()
                }),
            ) {
                error!("Error while saving tx_data for cNFT: {}", e);
            };
        }

        Ok(())
    }

    pub async fn mint_v1<'c>(
        &self,
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle<'c>,
    ) -> Result<(), IngesterError> {
        if let (
            Some(le),
            Some(cl),
            Some(Payload::MintV1 {
                args,
                authority,
                tree_id,
            }),
        ) = (
            &parsing_result.leaf_update,
            &parsing_result.tree_update,
            &parsing_result.payload,
        ) {
            self.rocks_client.save_changelog(cl, bundle.slot).await;

            let tree_id = Pubkey::new_from_array(tree_id.to_owned());
            //     Pubkey::new_from_array(bundle.keys.get(3).unwrap().0.to_vec().try_into().unwrap());
            let authority = Pubkey::new_from_array(authority.to_owned());
            //     Pubkey::new_from_array(bundle.keys.get(0).unwrap().0.to_vec().try_into().unwrap());

            match le.schema {
                LeafSchema::V1 {
                    id,
                    delegate,
                    owner,
                    nonce,
                    ..
                } => {
                    let chain_mutability = match args.is_mutable {
                        true => ChainMutability::Mutable,
                        false => ChainMutability::Immutable,
                    };

                    let mut chain_data = ChainDataV1 {
                        name: args.name.clone(),
                        symbol: args.symbol.clone(),
                        edition_nonce: args.edition_nonce,
                        primary_sale_happened: args.primary_sale_happened,
                        token_standard: Some(TokenStandard::NonFungible),
                        uses: args.uses.clone().map(|u| Uses {
                            use_method: use_method_from_mpl_bubblegum_state(&u.use_method),
                            remaining: u.remaining,
                            total: u.total,
                        }),
                        chain_mutability: Some(chain_mutability),
                    };
                    chain_data.sanitize();

                    let chain_data = json!(chain_data);
                    let asset_static_details = AssetStaticDetails {
                        pubkey: id,
                        specification_asset_class: SpecificationAssetClass::Nft,
                        royalty_target_type: RoyaltyTargetType::Creators,
                        created_at: bundle.slot as i64,
                    };

                    if let Err(e) = self
                        .rocks_client
                        .asset_static_data
                        .put(id, &asset_static_details)
                    {
                        error!("Error while saving static data for cNFT: {}", e);
                    };

                    let creators = {
                        let mut creators = vec![];
                        for creator in args.creators.iter() {
                            creators.push(Creator {
                                creator: creator.address,
                                creator_verified: creator.verified,
                                creator_share: creator.share,
                            });
                        }
                        creators
                    };

                    if let Err(e) = self.rocks_client.save_tx_data_and_asset_updated(
                        id,
                        bundle.slot,
                        Some(AssetLeaf {
                            pubkey: id,
                            tree_id,
                            leaf: Some(le.leaf_hash.to_vec()),
                            nonce: Some(nonce),
                            data_hash: Some(Hash::from(le.schema.data_hash())),
                            creator_hash: Some(Hash::from(le.schema.creator_hash())),
                            leaf_seq: Some(cl.seq),
                            slot_updated: bundle.slot,
                        }),
                        Some(AssetDynamicDetails {
                            pubkey: id,
                            is_compressed: Updated::new(bundle.slot, Some(cl.seq), true),
                            is_compressible: Updated::new(bundle.slot, Some(cl.seq), false),
                            supply: Some(Updated::new(bundle.slot, Some(cl.seq), 1)),
                            seq: Some(Updated::new(bundle.slot, Some(cl.seq), cl.seq)),
                            onchain_data: Some(Updated::new(
                                bundle.slot,
                                Some(cl.seq),
                                chain_data.to_string(),
                            )),
                            creators: Updated::new(bundle.slot, Some(cl.seq), creators),
                            royalty_amount: Updated::new(
                                bundle.slot,
                                Some(cl.seq),
                                args.seller_fee_basis_points,
                            ),
                            url: Updated::new(bundle.slot, Some(cl.seq), args.uri.clone()),
                            ..Default::default()
                        }),
                    ) {
                        error!("Error while saving tx_data for cNFT: {}", e);
                    }

                    let asset_authority = AssetAuthority {
                        pubkey: id,
                        authority,
                        slot_updated: bundle.slot,
                    };

                    // TODO: Do we really need put, not merge?
                    if let Err(e) = self
                        .rocks_client
                        .asset_authority_data
                        .put(id, &asset_authority)
                    {
                        error!("Error while saving authority for cNFT: {}", e);
                    };

                    if let Err(e) = self.rocks_client.asset_owner_data.put(
                        id,
                        &AssetOwner {
                            pubkey: id,
                            owner: Updated::new(bundle.slot, Some(cl.seq), owner),
                            delegate: get_delegate(delegate, owner, bundle.slot, cl.seq),
                            owner_type: Updated::new(bundle.slot, Some(cl.seq), OwnerType::Single),
                            owner_delegate_seq: Some(Updated::new(
                                bundle.slot,
                                Some(cl.seq),
                                cl.seq,
                            )),
                        },
                    ) {
                        error!("Error while saving owner for cNFT: {}", e);
                    };

                    if let Some(collection) = &args.collection {
                        let asset_collection = AssetCollection {
                            pubkey: id,
                            collection: collection.key,
                            is_collection_verified: collection.verified,
                            collection_seq: Some(cl.seq),
                            slot_updated: bundle.slot,
                        };

                        if let Err(e) = self
                            .rocks_client
                            .asset_collection_data
                            .merge(id, &asset_collection)
                        {
                            error!("Error while saving collection for cNFT: {}", e);
                        };
                    }
                }
            }

            let mut tasks_buffer = self.json_tasks.lock().await;

            let task = Task {
                ofd_metadata_url: args.uri.clone(),
                ofd_locked_until: Some(chrono::Utc::now()),
                ofd_attempts: 0,
                ofd_max_attempts: 10,
                ofd_error: None,
            };

            tasks_buffer.push_back(task);

            return Ok(());
        }
        Err(IngesterError::ParsingError(
            "Ix not parsed correctly".to_string(),
        ))
    }

    pub async fn redeem<'c>(
        &self,
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle<'c>,
    ) -> Result<(), IngesterError> {
        if let (Some(le), Some(cl)) = (&parsing_result.leaf_update, &parsing_result.tree_update) {
            self.rocks_client.save_changelog(cl, bundle.slot).await;

            match le.schema {
                LeafSchema::V1 { id, nonce, .. } => {
                    let tree = cl.id;
                    if let Err(e) = self.rocks_client.save_tx_data_and_asset_updated(
                        id,
                        bundle.slot,
                        Some(AssetLeaf {
                            pubkey: id,
                            tree_id: tree,
                            leaf: None,
                            nonce: Some(nonce),
                            data_hash: None,
                            creator_hash: None,
                            leaf_seq: Some(cl.seq),
                            slot_updated: bundle.slot,
                        }),
                        None,
                    ) {
                        error!("Error while saving tx_data for cNFT: {}", e);
                    };
                }
            }

            return Ok(());
        }
        Err(IngesterError::ParsingError(
            "Ix not parsed correctly".to_string(),
        ))
    }

    pub async fn decompress<'c>(
        &self,
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle<'c>,
    ) -> Result<(), IngesterError> {
        if let (Some(le), Some(cl)) = (&parsing_result.leaf_update, &parsing_result.tree_update) {
            match le.schema {
                LeafSchema::V1 { id, .. } => {
                    let tree = cl.id;
                    if let Err(e) = self.rocks_client.save_tx_data_and_asset_updated(
                        id,
                        bundle.slot,
                        Some(AssetLeaf {
                            pubkey: id,
                            tree_id: tree,
                            leaf: None,
                            nonce: None,
                            data_hash: None,
                            creator_hash: None,
                            leaf_seq: None,
                            slot_updated: bundle.slot,
                        }),
                        Some(AssetDynamicDetails {
                            pubkey: id,
                            was_decompressed: Updated::new(bundle.slot, Some(cl.seq), true),
                            seq: Some(Updated::new(bundle.slot, Some(cl.seq), cl.seq)),
                            is_compressible: Updated::new(bundle.slot, Some(cl.seq), true), // TODO
                            supply: Some(Updated::new(bundle.slot, Some(cl.seq), 1)),
                            ..Default::default()
                        }),
                    ) {
                        error!("Error while saving tx_data for cNFT: {}", e);
                    };
                }
            }

            return Ok(());
        }

        Err(IngesterError::ParsingError(
            "Ix not parsed correctly".to_string(),
        ))
    }

    // TODO: our impl have many difference from original one.
    // Starting from updated_creators and ended up with delegate field
    // Need to rewrite or discuss why there so many diffs
    pub async fn creator_verification<'c>(
        &self,
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle<'c>,
    ) -> Result<(), IngesterError> {
        if let (Some(le), Some(cl), Some(payload)) = (
            &parsing_result.leaf_update,
            &parsing_result.tree_update,
            &parsing_result.payload,
        ) {
            self.rocks_client.save_changelog(cl, bundle.slot).await;

            let (creator, verify) = match payload {
                Payload::CreatorVerification {
                    creator, verify, ..
                } => (creator, verify),
                _ => {
                    return Err(IngesterError::DatabaseError(
                        "Ix not parsed correctly".to_string(),
                    ));
                }
            };

            match le.schema {
                LeafSchema::V1 { id, nonce, .. } => {
                    let creator = Creator {
                        creator: *creator,
                        creator_verified: *verify,
                        creator_share: 0,
                    };

                    if let Err(e) = self.rocks_client.save_tx_data_and_asset_updated(
                        id,
                        bundle.slot,
                        Some(AssetLeaf {
                            pubkey: id,
                            tree_id: cl.id,
                            leaf: Some(le.leaf_hash.to_vec()),
                            nonce: Some(nonce),
                            data_hash: Some(Hash::from(le.schema.data_hash())),
                            creator_hash: Some(Hash::from(le.schema.creator_hash())),
                            leaf_seq: Some(cl.seq),
                            slot_updated: bundle.slot,
                        }),
                        Some(AssetDynamicDetails {
                            pubkey: id,
                            is_compressed: Updated::new(bundle.slot, Some(cl.seq), true),
                            supply: Some(Updated::new(bundle.slot, Some(cl.seq), 1)),
                            seq: Some(Updated::new(bundle.slot, Some(cl.seq), cl.seq)),
                            creators: Updated::new(bundle.slot, Some(cl.seq), vec![creator]),
                            ..Default::default()
                        }),
                    ) {
                        error!("Error while saving tx_data for cNFT: {}", e);
                    }
                }
            }

            return Ok(());
        }
        Err(IngesterError::ParsingError(
            "Ix not parsed correctly".to_string(),
        ))
    }

    pub async fn collection_verification<'c>(
        &self,
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle<'c>,
    ) -> Result<(), IngesterError> {
        if let (Some(le), Some(cl), Some(payload)) = (
            &parsing_result.leaf_update,
            &parsing_result.tree_update,
            &parsing_result.payload,
        ) {
            self.rocks_client.save_changelog(cl, bundle.slot).await;

            let (collection, verify) = match payload {
                Payload::CollectionVerification {
                    collection, verify, ..
                } => (*collection, *verify),
                _ => {
                    return Err(IngesterError::DatabaseError(
                        "Ix not parsed correctly".to_string(),
                    ));
                }
            };

            match le.schema {
                LeafSchema::V1 { id, .. } => {
                    if let Err(e) = self.rocks_client.save_tx_data_and_asset_updated(
                        id,
                        bundle.slot,
                        Some(AssetLeaf {
                            pubkey: id,
                            tree_id: cl.id,
                            leaf: Some(le.leaf_hash.to_vec()),
                            nonce: Some(cl.index as u64),
                            data_hash: Some(Hash::from(le.schema.data_hash())),
                            creator_hash: Some(Hash::from(le.schema.creator_hash())),
                            leaf_seq: Some(cl.seq),
                            slot_updated: bundle.slot,
                        }),
                        None,
                    ) {
                        error!("Error while saving tx_data for cNFT: {}", e);
                    }

                    let collection = AssetCollection {
                        pubkey: id,
                        collection,
                        is_collection_verified: verify,
                        collection_seq: Some(cl.seq),
                        slot_updated: bundle.slot,
                    };

                    if let Err(e) = self
                        .rocks_client
                        .asset_collection_data
                        .merge(id, &collection)
                    {
                        error!("Error while saving collection for cNFT: {}", e);
                    };
                }
            }

            return Ok(());
        };
        Err(IngesterError::ParsingError(
            "Ix not parsed correctly".to_string(),
        ))
    }

    pub async fn update_metadata<'c>(
        &self,
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle<'c>,
    ) -> Result<(), IngesterError> {
        if let (
            Some(le),
            Some(cl),
            Some(Payload::UpdateMetadata {
                current_metadata,
                update_args,
                tree_id,
            }),
        ) = (
            &parsing_result.leaf_update,
            &parsing_result.tree_update,
            &parsing_result.payload,
        ) {
            self.rocks_client.save_changelog(cl, bundle.slot).await;

            return match le.schema {
                LeafSchema::V1 { id, nonce, .. } => {
                    let uri = if let Some(uri) = &update_args.uri {
                        uri.replace('\0', "")
                    } else {
                        current_metadata.uri.replace('\0', "")
                    };
                    if uri.is_empty() {
                        return Err(IngesterError::DeserializationError(
                            "URI is empty".to_string(),
                        ));
                    }

                    let name = if let Some(name) = update_args.name.clone() {
                        name
                    } else {
                        current_metadata.name.clone()
                    };

                    let symbol = if let Some(symbol) = update_args.symbol.clone() {
                        symbol
                    } else {
                        current_metadata.symbol.clone()
                    };

                    let primary_sale_happened =
                        if let Some(primary_sale_happened) = update_args.primary_sale_happened {
                            primary_sale_happened
                        } else {
                            current_metadata.primary_sale_happened
                        };

                    let is_mutable = if let Some(is_mutable) = update_args.is_mutable {
                        is_mutable
                    } else {
                        current_metadata.is_mutable
                    };

                    let chain_mutability = if is_mutable {
                        ChainMutability::Mutable
                    } else {
                        ChainMutability::Immutable
                    };

                    let mut chain_data = ChainDataV1 {
                        name: name.clone(),
                        symbol: symbol.clone(),
                        edition_nonce: current_metadata.edition_nonce,
                        primary_sale_happened,
                        token_standard: Some(TokenStandard::NonFungible),
                        uses: current_metadata
                            .uses
                            .clone()
                            .map(|u| {
                                Ok::<_, IngesterError>(Uses {
                                    use_method: UseMethod::from_u8(u.use_method.clone() as u8)
                                        .ok_or(IngesterError::ParsingError(format!(
                                            "Invalid use_method: {}",
                                            u.use_method as u8
                                        )))?,
                                    remaining: u.remaining,
                                    total: u.total,
                                })
                            })
                            .transpose()?,
                        chain_mutability: Some(chain_mutability),
                    };
                    chain_data.sanitize();
                    let chain_data_json = serde_json::to_value(chain_data)
                        .map_err(|e| IngesterError::DeserializationError(e.to_string()))?;

                    let seller_fee_basis_points = if let Some(seller_fee_basis_points) =
                        update_args.seller_fee_basis_points
                    {
                        seller_fee_basis_points
                    } else {
                        current_metadata.seller_fee_basis_points
                    };

                    let creators_input = if let Some(creators) = &update_args.creators {
                        creators
                    } else {
                        &current_metadata.creators
                    };

                    let creators = {
                        let mut creators = vec![];
                        for creator in creators_input.iter() {
                            creators.push(Creator {
                                creator: creator.address,
                                creator_verified: creator.verified,
                                creator_share: creator.share,
                            });
                        }
                        creators
                    };

                    self.rocks_client.save_tx_data_and_asset_updated(
                        id,
                        bundle.slot,
                        Some(AssetLeaf {
                            pubkey: id,
                            tree_id: Pubkey::from(*tree_id),
                            leaf: Some(le.leaf_hash.to_vec()),
                            nonce: Some(nonce),
                            data_hash: Some(Hash::from(le.schema.data_hash())),
                            creator_hash: Some(Hash::from(le.schema.creator_hash())),
                            leaf_seq: Some(cl.seq),
                            slot_updated: bundle.slot,
                        }),
                        Some(AssetDynamicDetails {
                            pubkey: id,
                            onchain_data: Some(Updated {
                                slot_updated: bundle.slot,
                                seq: Some(cl.seq),
                                value: chain_data_json.to_string(),
                            }),
                            url: Updated {
                                slot_updated: bundle.slot,
                                seq: Some(cl.seq),
                                value: uri.clone(),
                            },
                            creators: Updated {
                                slot_updated: bundle.slot,
                                seq: Some(cl.seq),
                                value: creators,
                            },
                            royalty_amount: Updated::new(
                                bundle.slot,
                                Some(cl.seq),
                                seller_fee_basis_points,
                            ),
                            ..Default::default()
                        }),
                    )?;

                    let mut tasks_buffer = self.json_tasks.lock().await;
                    tasks_buffer.push_back(Task {
                        ofd_metadata_url: uri.clone(),
                        ofd_locked_until: Some(chrono::Utc::now()),
                        ofd_attempts: 0,
                        ofd_max_attempts: 10,
                        ofd_error: None,
                    });

                    Ok(())
                }
            };
        }
        Err(IngesterError::ParsingError(
            "Ix not parsed correctly".to_string(),
        ))
    }
}

fn use_method_from_mpl_bubblegum_state(
    value: &mpl_bubblegum::types::UseMethod,
) -> entities::enums::UseMethod {
    match value {
        mpl_bubblegum::types::UseMethod::Burn => entities::enums::UseMethod::Burn,
        mpl_bubblegum::types::UseMethod::Multiple => entities::enums::UseMethod::Multiple,
        mpl_bubblegum::types::UseMethod::Single => entities::enums::UseMethod::Single,
    }
}

fn get_delegate(delegate: Pubkey, owner: Pubkey, slot: u64, seq: u64) -> Option<Updated<Pubkey>> {
    let delegate = if owner == delegate || delegate.to_bytes() == [0; 32] {
        None
    } else {
        Some(delegate)
    };

    delegate.map(|delegate| Updated::new(slot, Some(seq), delegate))
}
