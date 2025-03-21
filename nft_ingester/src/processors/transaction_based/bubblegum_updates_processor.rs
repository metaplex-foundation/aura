use std::{
    collections::{HashSet, VecDeque},
    str::FromStr,
    sync::Arc,
};

use blockbuster::{
    instruction::{order_instructions, InstructionBundle, IxPair},
    program_handler::ProgramParser,
    programs::{
        bubblegum::{BubblegumInstruction, BubblegumParser, Payload},
        ProgramParseResult,
    },
};
use bubblegum_batch_sdk::model::BatchMint;
use chrono::Utc;
use entities::{
    enums::{
        ChainMutability, OwnerType, PersistingBatchMintState, RoyaltyTargetType,
        SpecificationAssetClass, TokenStandard, UseMethod,
    },
    models::{
        BatchMintToVerify, BufferedTransaction, ChainDataV1, Creator, SignatureWithSlot,
        TransactionInfo, UpdateVersion, Updated, Uses,
    },
};
use lazy_static::lazy_static;
use metrics_utils::IngesterMetricsConfig;
use mpl_bubblegum::{types::LeafSchema, InstructionName};
use num_traits::FromPrimitive;
use rocks_db::{
    columns::{
        asset::{
            AssetAuthority, AssetCollection, AssetDynamicDetails, AssetLeaf, AssetOwner,
            AssetStaticDetails,
        },
        offchain_data::OffChainData,
    },
    transaction::{
        AssetDynamicUpdate, AssetUpdate, AssetUpdateEvent, InstructionResult, TransactionResult,
        TreeUpdate,
    },
    Storage,
};
use serde_json::json;
use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature};
use tokio::time::Instant;
use tracing::{debug, error};
use usecase::save_metrics::result_to_metrics;

use crate::{
    error::IngesterError, flatbuffer_mapper::FlatbufferMapper, plerkle::PlerkleTransactionInfo,
};

pub const BUFFER_PROCESSING_COUNTER: i32 = 10;
const BATCH_MINT_BATCH_FLUSH_SIZE: usize = 10_000;
lazy_static! {
    static ref KEY_SET: HashSet<Pubkey> = {
        let mut m = HashSet::new();
        m.insert(BubblegumParser {}.key());
        m
    };
}

#[derive(Clone)]
pub struct BubblegumTxProcessor {
    pub transaction_parser: Arc<FlatbufferMapper>,
    pub instruction_parser: Arc<BubblegumParser>,
    pub rocks_client: Arc<rocks_db::Storage>,

    pub metrics: Arc<IngesterMetricsConfig>,
}

impl BubblegumTxProcessor {
    pub fn new(rocks_client: Arc<rocks_db::Storage>, metrics: Arc<IngesterMetricsConfig>) -> Self {
        BubblegumTxProcessor {
            transaction_parser: Arc::new(FlatbufferMapper {}),
            instruction_parser: Arc::new(BubblegumParser {}),
            rocks_client,
            metrics,
        }
    }

    pub fn break_transaction(tx_info: &TransactionInfo) -> VecDeque<(IxPair, Option<Vec<IxPair>>)> {
        order_instructions(
            &KEY_SET,
            tx_info.account_keys.as_slice(),
            tx_info.message_instructions.as_slice(),
            tx_info.meta_inner_instructions.as_slice(),
        )
    }

    pub async fn process_transaction(
        &self,
        data: BufferedTransaction,
        is_from_finalized_source: bool,
    ) -> Result<(), IngesterError> {
        if data == BufferedTransaction::default() {
            return Ok(());
        }
        let begin_processing = Instant::now();
        let data = Self::parse_transaction_info_from_fb(data, self.transaction_parser.clone())?;
        let result = Self::get_handle_transaction_results(
            self.instruction_parser.clone(),
            data,
            self.metrics.clone(),
        )?;

        let res = self
            .rocks_client
            .store_transaction_result(&result, true, is_from_finalized_source)
            .await
            .map_err(|e| IngesterError::DatabaseError(e.to_string()));

        result_to_metrics(self.metrics.clone(), &res, "process_transaction");
        self.metrics
            .set_latency("process_transaction", begin_processing.elapsed().as_millis() as f64);

        res
    }

    fn instruction_name_to_string(ix: &InstructionName) -> &'static str {
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
            InstructionName::PrepareTree => "PrepareTree",
            InstructionName::AddCanopy => "AddCanopy",
            InstructionName::FinalizeTreeWithRoot => "FinalizeTreeWithRoot",
            InstructionName::FinalizeTreeWithRootAndCollection => {
                "FinalizeTreeWithRootAndCollection"
            },
        }
    }

    // PDA lookup requires an 8-byte array.
    pub fn u32_to_u8_array(value: u32) -> [u8; 8] {
        let bytes: [u8; 4] = value.to_le_bytes();
        let mut result: [u8; 8] = [0; 8];
        result[..4].copy_from_slice(&bytes);
        result
    }
    pub fn parse_transaction_info_from_fb(
        data: BufferedTransaction,
        transaction_parser: Arc<FlatbufferMapper>,
    ) -> Result<TransactionInfo, IngesterError> {
        let seen_at = Utc::now();

        let mut transaction_info_bytes = data.transaction.clone();

        if data.map_flatbuffer {
            let tx_update =
                    utils::flatbuffer::transaction_info_generated::transaction_info::root_as_transaction_info(
                        &data.transaction,
                    ).unwrap();
            transaction_info_bytes =
                transaction_parser.map_tx_fb_bytes(tx_update, seen_at).unwrap();
        }
        let transaction_info =
            plerkle_serialization::root_as_transaction_info(transaction_info_bytes.as_slice())
                .unwrap();

        Ok(PlerkleTransactionInfo(transaction_info).try_into()?)
    }
    pub fn get_handle_transaction_results(
        instruction_parser: Arc<BubblegumParser>,
        tx: TransactionInfo,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<TransactionResult, IngesterError> {
        let sig = tx.signature;
        let instructions = Self::break_transaction(&tx);
        let slot = tx.slot;
        let signature = tx.signature;
        let mut transaction_result = TransactionResult {
            instruction_results: vec![],
            transaction_signature: Some((
                mpl_bubblegum::programs::MPL_BUBBLEGUM_ID,
                SignatureWithSlot { signature, slot },
            )),
        };

        for (outer_ix, inner_ix) in instructions {
            let (program, instruction) = outer_ix;
            if program != mpl_bubblegum::programs::MPL_BUBBLEGUM_ID {
                continue;
            }

            let ix_accounts = &instruction.accounts;
            let ix_account_len = ix_accounts.len();
            let max = ix_accounts.iter().max().copied().unwrap_or(0) as usize;
            if tx.account_keys.len() < max {
                return Err(IngesterError::DeserializationError(
                    "Missing Accounts in Serialized Ixn/Txn".to_string(),
                ));
            }
            let ix_accounts =
                ix_accounts.iter().fold(Vec::with_capacity(ix_account_len), |mut acc, a| {
                    if let Some(key) = tx.account_keys.get(*a as usize) {
                        acc.push(*key);
                    }
                    acc
                });
            let ix = InstructionBundle {
                txn_id: &signature.to_string(),
                program,
                instruction: Some(instruction),
                inner_ix: inner_ix.as_deref(),
                keys: ix_accounts.as_slice(),
                slot,
            };

            let result = instruction_parser.handle_instruction(&ix)?;
            if let ProgramParseResult::Bubblegum(parsing_result) = result.result_type() {
                metrics.inc_instructions(Self::instruction_name_to_string(
                    &parsing_result.instruction,
                ));

                let ix_parse_res = Self::get_bubblegum_instruction_update(parsing_result, &ix);

                match ix_parse_res {
                    Ok(ix_result) => {
                        transaction_result.instruction_results.push(ix_result);
                    },
                    Err(e) => {
                        error!("Failed to handle bubblegum instruction for txn {}: {:?}", sig, e);
                        return Err(IngesterError::TransactionParsingError(format!(
                            "Failed to parse transaction: {:?}",
                            sig
                        )));
                    },
                };
            }
        }

        metrics.set_last_processed_slot("transaction", slot as i64);

        Ok(transaction_result)
    }

    pub fn get_bubblegum_instruction_update<'c>(
        parsing_result: &'c BubblegumInstruction,
        bundle: &'c InstructionBundle<'c>,
    ) -> Result<InstructionResult, IngesterError> {
        let ix_type = &parsing_result.instruction;
        let ix_str = Self::instruction_name_to_string(ix_type);
        debug!("BGUM instruction txn={:?}: {:?}", ix_str, bundle.txn_id);

        let mut tree_update = None;
        if let Some(cl) = &parsing_result.tree_update {
            tree_update = Some(TreeUpdate {
                tree: cl.id,
                seq: cl.seq,
                slot: bundle.slot,
                event: cl.into(),
                instruction: ix_str.to_string(),
                tx: bundle.txn_id.to_string(),
            });
        };

        let instruction: Result<InstructionResult, IngesterError> = match ix_type {
            InstructionName::Transfer
            | InstructionName::CancelRedeem
            | InstructionName::Delegate => {
                Self::get_update_owner_update(parsing_result, bundle).map(From::from).map(Ok)?
            },
            InstructionName::Burn => {
                Self::get_burn_update(parsing_result, bundle).map(From::from).map(Ok)?
            },
            InstructionName::MintV1 | InstructionName::MintToCollectionV1 => {
                Self::get_mint_v1_update(parsing_result, bundle.slot).map(From::from).map(Ok)?
            },
            InstructionName::Redeem => {
                Self::get_redeem_update(parsing_result, bundle).map(From::from).map(Ok)?
            },
            InstructionName::DecompressV1 => Ok(Self::get_decompress_update(bundle).into()), // no change log here? really?
            InstructionName::VerifyCreator | InstructionName::UnverifyCreator => {
                Self::get_creator_verification_update(parsing_result, bundle)
                    .map(From::from)
                    .map(Ok)?
            },
            InstructionName::VerifyCollection
            | InstructionName::UnverifyCollection
            | InstructionName::SetAndVerifyCollection => {
                Self::get_collection_verification_update(parsing_result, bundle)
                    .map(From::from)
                    .map(Ok)?
            },
            InstructionName::UpdateMetadata => {
                Self::get_update_metadata_update(parsing_result, bundle).map(From::from).map(Ok)?
            },
            InstructionName::FinalizeTreeWithRoot
            | InstructionName::FinalizeTreeWithRootAndCollection => {
                Self::get_create_tree_with_root_update(parsing_result, bundle)
                    .map(From::from)
                    .map(Ok)?
            },
            _ => {
                debug!("Bubblegum: Not Implemented Instruction");
                Ok(InstructionResult::default())
            }, // InstructionName::Unknown => todo!(),
               // InstructionName::Compress => todo!(),
               // InstructionName::CreateTree => todo!(),
               // InstructionName::SetDecompressibleState => todo!(),
        };
        let mut instruction = instruction?;
        instruction.tree_update = tree_update;
        Ok(instruction)
    }

    pub fn get_create_tree_with_root_update(
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle,
    ) -> Result<AssetUpdateEvent, IngesterError> {
        if let Some(Payload::FinalizeTreeWithRoot { args, .. }) = &parsing_result.payload {
            let upd = AssetUpdateEvent {
                batch_mint_creation_update: Some(BatchMintToVerify {
                    file_hash: args.metadata_hash.clone(),
                    url: args.metadata_url.clone(),
                    created_at_slot: bundle.slot,
                    signature: Signature::from_str(bundle.txn_id)
                        .map_err(|e| IngesterError::ParseSignatureError(e.to_string()))?,
                    download_attempts: 0,
                    persisting_state: PersistingBatchMintState::ReceivedTransaction,
                    staker: args.staker,
                    collection_mint: args.collection_mint,
                }),
                ..Default::default()
            };

            return Ok(upd);
        }

        Err(IngesterError::ParsingError("Ix not parsed correctly".to_string()))
    }

    pub fn get_update_owner_update(
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle,
    ) -> Result<AssetUpdateEvent, IngesterError> {
        if let (Some(le), Some(cl)) = (&parsing_result.leaf_update, &parsing_result.tree_update) {
            match le.schema {
                LeafSchema::V1 { id, owner, delegate, .. } => {
                    let leaf = Some(AssetLeaf {
                        pubkey: id,
                        tree_id: cl.id,
                        leaf: Some(le.leaf_hash.to_vec()),
                        nonce: Some(cl.index as u64),
                        data_hash: Some(Hash::from(le.schema.data_hash())),
                        creator_hash: Some(Hash::from(le.schema.creator_hash())),
                        leaf_seq: Some(cl.seq),
                        slot_updated: bundle.slot,
                    });
                    let owner = AssetOwner {
                        pubkey: id,
                        owner: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            Some(owner),
                        ),
                        delegate: get_delegate(delegate, owner, bundle.slot, cl.seq),
                        owner_type: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            OwnerType::Single,
                        ),
                        owner_delegate_seq: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            Some(cl.seq),
                        ),
                        is_current_owner: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            true,
                        ),
                    };
                    let asset_update = AssetUpdateEvent {
                        update: Some(AssetDynamicUpdate {
                            pk: id,
                            slot: bundle.slot,
                            leaf,
                            dynamic_data: None,
                        }),
                        owner_update: Some(AssetUpdate { pk: id, details: owner }),
                        ..Default::default()
                    };
                    return Ok(asset_update);
                },
            }
        }
        Err(IngesterError::ParsingError("Ix not parsed correctly".to_string()))
    }

    pub fn get_burn_update(
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle,
    ) -> Result<AssetUpdateEvent, IngesterError> {
        if let Some(cl) = &parsing_result.tree_update {
            let (asset_id, _) = Pubkey::find_program_address(
                &["asset".as_bytes(), cl.id.as_ref(), Self::u32_to_u8_array(cl.index).as_ref()],
                &mpl_bubblegum::ID,
            );

            let asset_update = AssetUpdateEvent {
                update: Some(AssetDynamicUpdate {
                    pk: asset_id,
                    slot: bundle.slot,
                    leaf: None,
                    dynamic_data: Some(AssetDynamicDetails {
                        pubkey: asset_id,
                        supply: Some(Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            0,
                        )),
                        is_burnt: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            true,
                        ),
                        seq: Some(Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            cl.seq,
                        )),
                        is_compressed: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            true,
                        ),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            };

            return Ok(asset_update);
        }

        Err(IngesterError::ParsingError("Ix not parsed correctly".to_string()))
    }

    pub fn get_mint_v1_update(
        parsing_result: &BubblegumInstruction,
        slot: u64,
    ) -> Result<AssetUpdateEvent, IngesterError> {
        if let (Some(le), Some(cl), Some(Payload::MintV1 { args, authority, tree_id })) =
            (&parsing_result.leaf_update, &parsing_result.tree_update, &parsing_result.payload)
        {
            let mut asset_update = AssetUpdateEvent { ..Default::default() };

            let uri = args.uri.trim().replace('\0', "");
            match le.schema {
                LeafSchema::V1 { id, delegate, owner, nonce, .. } => {
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
                    };
                    chain_data.sanitize();

                    let chain_data = json!(chain_data);
                    let asset_static_details = AssetStaticDetails {
                        pubkey: id,
                        specification_asset_class: SpecificationAssetClass::Nft,
                        royalty_target_type: RoyaltyTargetType::Creators,
                        created_at: slot as i64,
                        edition_address: None,
                    };
                    asset_update.static_update =
                        Some(AssetUpdate { pk: id, details: asset_static_details });

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
                    asset_update.update = Some(AssetDynamicUpdate {
                        pk: id,
                        slot,
                        leaf: Some(AssetLeaf {
                            pubkey: id,
                            tree_id: *tree_id,
                            leaf: Some(le.leaf_hash.to_vec()),
                            nonce: Some(nonce),
                            data_hash: Some(Hash::from(le.schema.data_hash())),
                            creator_hash: Some(Hash::from(le.schema.creator_hash())),
                            leaf_seq: Some(cl.seq),
                            slot_updated: slot,
                        }),
                        dynamic_data: Some(AssetDynamicDetails {
                            pubkey: id,
                            is_compressed: Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                true,
                            ),
                            is_compressible: Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                false,
                            ),
                            supply: Some(Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                1,
                            )),
                            seq: Some(Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                cl.seq,
                            )),
                            onchain_data: Some(Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                chain_data.to_string(),
                            )),
                            creators: Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                creators,
                            ),
                            royalty_amount: Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                args.seller_fee_basis_points,
                            ),
                            url: Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                uri.clone(),
                            ),
                            chain_mutability: Some(Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                chain_mutability,
                            )),
                            ..Default::default()
                        }),
                    });

                    let asset_authority = AssetAuthority {
                        pubkey: id,
                        authority: *authority,
                        slot_updated: slot,
                        write_version: None,
                    };
                    asset_update.authority_update =
                        Some(AssetUpdate { pk: id, details: asset_authority });
                    let owner = AssetOwner {
                        pubkey: id,
                        owner: Updated::new(
                            slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            Some(owner),
                        ),
                        delegate: get_delegate(delegate, owner, slot, cl.seq),
                        owner_type: Updated::new(
                            slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            OwnerType::Single,
                        ),
                        owner_delegate_seq: Updated::new(
                            slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            Some(cl.seq),
                        ),
                        is_current_owner: Updated::new(
                            slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            true,
                        ),
                    };
                    asset_update.owner_update = Some(AssetUpdate { pk: id, details: owner });

                    if let Some(collection) = &args.collection {
                        let asset_collection = AssetCollection {
                            pubkey: id,
                            collection: Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                collection.key,
                            ),
                            is_collection_verified: Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                collection.verified,
                            ),
                            authority: Updated::new(
                                slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                None,
                            ),
                        };
                        asset_update.collection_update =
                            Some(AssetUpdate { pk: id, details: asset_collection });
                    }
                },
            }

            return Ok(asset_update);
        }
        Err(IngesterError::ParsingError("Ix not parsed correctly".to_string()))
    }

    pub fn get_redeem_update(
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle,
    ) -> Result<AssetUpdateEvent, IngesterError> {
        if let Some(cl) = &parsing_result.tree_update {
            let leaf_index = cl.index;
            let (asset_id, _) = Pubkey::find_program_address(
                &["asset".as_bytes(), cl.id.as_ref(), Self::u32_to_u8_array(leaf_index).as_ref()],
                &mpl_bubblegum::ID,
            );

            let nonce = cl.index as u64;

            let asset_update = AssetUpdateEvent {
                update: Some(AssetDynamicUpdate {
                    pk: asset_id,
                    slot: bundle.slot,
                    leaf: Some(AssetLeaf {
                        pubkey: asset_id,
                        tree_id: cl.id,
                        leaf: Some(vec![0; 32]),
                        nonce: Some(nonce),
                        data_hash: Some(Hash::from([0; 32])),
                        creator_hash: Some(Hash::from([0; 32])),
                        leaf_seq: Some(cl.seq),
                        slot_updated: bundle.slot,
                    }),
                    dynamic_data: None,
                }),
                ..Default::default()
            };
            return Ok(asset_update);
        }

        Err(IngesterError::ParsingError("Ix not parsed correctly".to_string()))
    }

    pub fn get_decompress_update(bundle: &InstructionBundle) -> AssetUpdate<AssetDynamicDetails> {
        let asset_id = bundle.keys.get(3).unwrap();
        AssetUpdate {
            pk: *asset_id,
            details: AssetDynamicDetails {
                pubkey: *asset_id,
                was_decompressed: Some(Updated::new(bundle.slot, None, true)),
                is_compressible: Updated::new(bundle.slot, None, false),
                supply: Some(Updated::new(bundle.slot, None, 1)),
                seq: Some(Updated::new(bundle.slot, None, 0)),
                ..Default::default()
            },
        }
    }

    pub fn get_creator_verification_update(
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle,
    ) -> Result<AssetUpdateEvent, IngesterError> {
        if let (Some(le), Some(cl), Some(payload)) =
            (&parsing_result.leaf_update, &parsing_result.tree_update, &parsing_result.payload)
        {
            let updated_creators = match payload {
                Payload::CreatorVerification { metadata, creator, verify } => {
                    let updated_creators: Vec<Creator> = metadata
                        .creators
                        .iter()
                        .map(|c| {
                            let mut c = Creator {
                                creator: c.address,
                                creator_verified: c.verified,
                                creator_share: c.share,
                            };

                            if c.creator == *creator {
                                c.creator_verified = *verify
                            };
                            c
                        })
                        .collect();

                    updated_creators
                },
                _ => {
                    return Err(IngesterError::ParsingError("Ix not parsed correctly".to_string()));
                },
            };
            let mut asset_update = AssetUpdateEvent { ..Default::default() };
            match le.schema {
                LeafSchema::V1 { id, owner, delegate, .. } => {
                    asset_update.update = Some(AssetDynamicUpdate {
                        pk: id,
                        slot: bundle.slot,
                        leaf: Some(AssetLeaf {
                            pubkey: id,
                            tree_id: cl.id,
                            leaf: Some(le.leaf_hash.to_vec()),
                            nonce: Some(cl.index as u64),
                            data_hash: Some(Hash::from(le.schema.data_hash())),
                            creator_hash: Some(Hash::from(le.schema.creator_hash())),
                            leaf_seq: Some(cl.seq),
                            slot_updated: bundle.slot,
                        }),
                        dynamic_data: Some(AssetDynamicDetails {
                            pubkey: id,
                            is_compressed: Updated::new(
                                bundle.slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                true,
                            ),
                            supply: Some(Updated::new(
                                bundle.slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                1,
                            )),
                            seq: Some(Updated::new(
                                bundle.slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                cl.seq,
                            )),
                            creators: Updated::new(
                                bundle.slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                updated_creators,
                            ),
                            ..Default::default()
                        }),
                    });

                    let owner = AssetOwner {
                        pubkey: id,
                        owner: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            Some(owner),
                        ),
                        delegate: get_delegate(delegate, owner, bundle.slot, cl.seq),
                        owner_type: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            OwnerType::Single,
                        ),
                        owner_delegate_seq: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            Some(cl.seq),
                        ),
                        is_current_owner: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            true,
                        ),
                    };
                    asset_update.owner_update = Some(AssetUpdate { pk: id, details: owner });
                },
            }

            return Ok(asset_update);
        }
        Err(IngesterError::ParsingError("Ix not parsed correctly".to_string()))
    }

    pub fn get_collection_verification_update(
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle,
    ) -> Result<AssetUpdateEvent, IngesterError> {
        if let (Some(le), Some(cl), Some(payload)) =
            (&parsing_result.leaf_update, &parsing_result.tree_update, &parsing_result.payload)
        {
            let mut asset_update = AssetUpdateEvent { ..Default::default() };

            let (collection, verify) = match payload {
                Payload::CollectionVerification { collection, verify, .. } => {
                    (*collection, *verify)
                },
                _ => {
                    return Err(IngesterError::DatabaseError(
                        "Ix not parsed correctly".to_string(),
                    ));
                },
            };

            match le.schema {
                LeafSchema::V1 { id, .. } => {
                    asset_update.update = Some(AssetDynamicUpdate {
                        pk: id,
                        slot: bundle.slot,
                        leaf: Some(AssetLeaf {
                            pubkey: id,
                            tree_id: cl.id,
                            leaf: Some(le.leaf_hash.to_vec()),
                            nonce: Some(cl.index as u64),
                            data_hash: Some(Hash::from(le.schema.data_hash())),
                            creator_hash: Some(Hash::from(le.schema.creator_hash())),
                            leaf_seq: Some(cl.seq),
                            slot_updated: bundle.slot,
                        }),
                        dynamic_data: None,
                    });

                    let collection = AssetCollection {
                        pubkey: id,
                        collection: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            collection,
                        ),
                        is_collection_verified: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            verify,
                        ),
                        authority: Updated::new(
                            bundle.slot,
                            Some(UpdateVersion::Sequence(cl.seq)),
                            None,
                        ),
                    };

                    asset_update.collection_update =
                        Some(AssetUpdate { pk: id, details: collection });
                },
            }

            return Ok(asset_update);
        };
        Err(IngesterError::ParsingError("Ix not parsed correctly".to_string()))
    }

    pub fn get_update_metadata_update(
        parsing_result: &BubblegumInstruction,
        bundle: &InstructionBundle,
    ) -> Result<AssetUpdateEvent, IngesterError> {
        if let (
            Some(le),
            Some(cl),
            Some(Payload::UpdateMetadata { current_metadata, update_args, tree_id }),
        ) = (&parsing_result.leaf_update, &parsing_result.tree_update, &parsing_result.payload)
        {
            let mut asset_update = AssetUpdateEvent { ..Default::default() };

            return match le.schema {
                LeafSchema::V1 { id, nonce, .. } => {
                    let uri = if let Some(uri) = &update_args.uri {
                        uri.replace('\0', "")
                    } else {
                        current_metadata.uri.replace('\0', "")
                    };

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

                    asset_update.update = Some(AssetDynamicUpdate {
                        pk: id,
                        slot: bundle.slot,
                        leaf: Some(AssetLeaf {
                            pubkey: id,
                            tree_id: *tree_id,
                            leaf: Some(le.leaf_hash.to_vec()),
                            nonce: Some(nonce),
                            data_hash: Some(Hash::from(le.schema.data_hash())),
                            creator_hash: Some(Hash::from(le.schema.creator_hash())),
                            leaf_seq: Some(cl.seq),
                            slot_updated: bundle.slot,
                        }),
                        dynamic_data: Some(AssetDynamicDetails {
                            pubkey: id,
                            onchain_data: Some(Updated::new(
                                bundle.slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                chain_data_json.to_string(),
                            )),
                            url: Updated::new(
                                bundle.slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                uri.clone(),
                            ),
                            creators: Updated::new(
                                bundle.slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                creators,
                            ),
                            royalty_amount: Updated::new(
                                bundle.slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                seller_fee_basis_points,
                            ),
                            chain_mutability: Some(Updated::new(
                                bundle.slot,
                                Some(UpdateVersion::Sequence(cl.seq)),
                                chain_mutability,
                            )),
                            ..Default::default()
                        }),
                    });

                    Ok(asset_update)
                },
            };
        }
        Err(IngesterError::ParsingError("Ix not parsed correctly".to_string()))
    }

    pub async fn store_batch_mint_update(
        slot: u64,
        batch_mint: &BatchMint,
        rocks_db: Arc<Storage>,
        signature: Signature,
    ) -> Result<(), IngesterError> {
        let mut transaction_result = TransactionResult {
            instruction_results: vec![],
            transaction_signature: Some((
                mpl_bubblegum::programs::MPL_BUBBLEGUM_ID,
                SignatureWithSlot { signature, slot },
            )),
        };
        for batched_mint in batch_mint.batch_mints.iter() {
            let seq = batched_mint.tree_update.seq;
            let event = (&spl_account_compression::events::ChangeLogEventV1::from(
                &batched_mint.tree_update,
            ))
                .into();
            let mut update = Self::get_mint_v1_update(&batched_mint.into(), slot)?;

            if let Some(dynamic_info) = &update.update {
                if let Some(data) = &dynamic_info.dynamic_data {
                    let url = data.url.value.clone();

                    if let Some(metadata) = batch_mint.raw_metadata_map.get(&url) {
                        update.offchain_data_update = Some(OffChainData {
                            url: Some(url.clone()),
                            metadata: Some(metadata.to_string()),
                            storage_mutability: url.as_str().into(),
                            last_read_at: Utc::now().timestamp(),
                        });
                    }
                }
            }

            let mut ix: InstructionResult = update.into();
            ix.tree_update = Some(TreeUpdate {
                tree: batched_mint.tree_update.id,
                seq,
                slot,
                event,
                instruction: "".to_string(),
                tx: signature.to_string(),
            });

            transaction_result.instruction_results.push(ix);
            if transaction_result.instruction_results.len() >= BATCH_MINT_BATCH_FLUSH_SIZE {
                rocks_db.store_transaction_result(&transaction_result, false, false).await?;
                transaction_result.instruction_results.clear();
            }
        }
        rocks_db.store_transaction_result(&transaction_result, true, false).await?;

        Ok(())
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

fn get_delegate(delegate: Pubkey, owner: Pubkey, slot: u64, seq: u64) -> Updated<Option<Pubkey>> {
    let delegate =
        if owner == delegate || delegate.to_bytes() == [0; 32] { None } else { Some(delegate) };

    Updated::new(slot, Some(UpdateVersion::Sequence(seq)), delegate)
}
