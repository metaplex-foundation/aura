use crate::cl_items::ClItem;
use crate::column::TypedColumn;
use crate::columns::TokenAccount;
use crate::{
    key_encoders::decode_pubkey,
    storage_traits::{AssetIndexReader, Dumper},
    Storage,
};
use async_trait::async_trait;
use bincode::deserialize;
use chrono::{DateTime, Utc};
use csv::{Writer, WriterBuilder};
use entities::enums::{
    OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions,
};
use entities::models::AssetSignature;
use hex;
use inflector::Inflector;
use serde::{Serialize, Serializer};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::ops::AddAssign;
use std::str::FromStr;
use std::{
    collections::HashSet,
    fs::File,
    io::{BufWriter, Write},
};

const ONE_G: usize = 1024 * 1024 * 1024;
fn serialize_as_snake_case<S, T>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: std::fmt::Debug, // Enums can be Debug-formatted to get their variant names
{
    let variant_name = format!("{:?}", value); // Get the variant name as a string
    let snake_case_name = variant_name.to_snake_case(); // Convert to snake_case
    serializer.serialize_str(&snake_case_name)
}

fn serialize_option_as_snake_case<S, T>(value: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: std::fmt::Debug, // Assumes T can be formatted using Debug, which is true for enums
{
    match value {
        Some(v) => {
            let variant_name = format!("{:?}", v); // Convert the enum variant to a string
            let snake_case_name = variant_name.to_snake_case(); // Convert to snake_case
            serializer.serialize_some(&snake_case_name)
        }
        None => serializer.serialize_none(),
    }
}
#[derive(Serialize)]
struct AssetRecord {
    ast_pubkey: String,
    #[serde(serialize_with = "serialize_as_snake_case")]
    ast_specification_version: SpecificationVersions,
    #[serde(serialize_with = "serialize_as_snake_case")]
    ast_specification_asset_class: SpecificationAssetClass,
    #[serde(serialize_with = "serialize_as_snake_case")]
    ast_royalty_target_type: RoyaltyTargetType,
    ast_royalty_amount: i64,
    ast_slot_created: i64,
    #[serde(serialize_with = "serialize_option_as_snake_case")]
    ast_owner_type: Option<OwnerType>,
    ast_owner: Option<String>,
    ast_delegate: Option<String>,
    ast_authority: Option<String>,
    ast_collection: Option<String>,
    ast_is_collection_verified: Option<bool>,
    ast_is_burnt: bool,
    ast_is_compressible: bool,
    ast_is_compressed: bool,
    ast_is_frozen: bool,
    ast_supply: Option<i64>,
    ast_metadata_url_id: Option<String>,
    ast_slot_updated: i64,
}

#[derive(Serialize)]
struct ReferenceAssetRecord {
    ast_pubkey: String,
    alt_id: String,
    #[serde(serialize_with = "serialize_as_snake_case")]
    specification_version: SpecificationVersions,
    #[serde(serialize_with = "serialize_as_snake_case")]
    specification_asset_class: SpecificationAssetClass,
    owner: Option<String>,
    #[serde(serialize_with = "serialize_option_as_snake_case")]
    owner_type: Option<OwnerType>,
    delegate: Option<String>,
    frozen: bool,
    supply: Option<i64>,
    supply_mint: String,
    compressed: bool,
    compressible: bool,
    seq: i64,
    tree_id: Option<String>,
    leaf: Option<String>,
    nonce: Option<u64>,
    #[serde(serialize_with = "serialize_as_snake_case")]
    royalty_target_type: RoyaltyTargetType,
    royalty_target: Option<String>,
    royalty_amount: i64,
    asset_data: Option<String>,
    created_at: DateTime<Utc>,
    data_hash: Option<String>,
    creator_hash: Option<String>,
    owner_delegate_seq: i64,
    leaf_seq: i64,
    base_info_seq: i64,
    slot_updated_metadata_account: i64,
    slot_updated_token_account: i64,
    slot_updated_mint_account: i64,
    slot_updated_cnft_transaction: i64,
    mpl_core_plugins: Option<String>,
    mpl_core_unknown_plugins: Option<String>,
    mpl_core_collection_num_minted: Option<u32>,
    mpl_core_collection_current_size: Option<u32>,
    mpl_core_plugins_json_version: Option<u32>,
}

impl Storage {
    pub async fn dump_csv<W: Write>(
        &self,
        metadata_writer: &mut Writer<W>,
        creators_writer: &mut Writer<W>,
        assets_writer: &mut Writer<W>,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), String> {
        let iter = self.asset_static_data.iter_start();
        // collect batch of keys
        let mut metadata_key_set = HashSet::new();
        let mut batch = Vec::with_capacity(batch_size);
        for k in iter
            .filter_map(|k| k.ok())
            .filter_map(|(key, _)| decode_pubkey(key.to_vec()).ok())
        {
            batch.push(k);
            if batch.len() == batch_size {
                self.dump_csv_batch(
                    &batch,
                    metadata_writer,
                    creators_writer,
                    assets_writer,
                    &mut metadata_key_set,
                )
                .await?;
                batch.clear();
            }
            if !rx.is_empty() {
                return Err("dump cancelled".to_string());
            }
        }
        if !batch.is_empty() {
            self.dump_csv_batch(
                &batch,
                metadata_writer,
                creators_writer,
                assets_writer,
                &mut metadata_key_set,
            )
            .await?;
        }
        metadata_writer.flush().map_err(|e| e.to_string())?;
        creators_writer.flush().map_err(|e| e.to_string())?;
        assets_writer.flush().map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn dump_csv_batch<W: Write>(
        &self,
        batch: &[Pubkey],
        metadata_writer: &mut csv::Writer<W>,
        creators_writer: &mut csv::Writer<W>,
        assets_writer: &mut csv::Writer<W>,
        metadata_key_set: &mut HashSet<Vec<u8>>,
    ) -> Result<(), String> {
        let indexes = self
            .get_asset_indexes(batch)
            .await
            .map_err(|e| e.to_string())?;
        for (key, index) in indexes {
            let metadata_url = index
                .metadata_url
                .map(|url| (url.get_metadata_id(), url.metadata_url.trim().to_owned()));
            if let Some((ref metadata_key, ref url)) = metadata_url {
                if !metadata_key_set.contains(metadata_key) {
                    metadata_key_set.insert(metadata_key.clone());
                    metadata_writer
                        .serialize((Self::encode(metadata_key), url.to_string(), "pending"))
                        .map_err(|e| e.to_string())?;
                }
            }
            for creator in index.creators {
                creators_writer
                    .serialize((
                        Self::encode(key.to_bytes()),
                        Self::encode(creator.creator),
                        creator.creator_verified,
                        index.slot_updated,
                    ))
                    .map_err(|e| e.to_string())?;
            }
            let record = AssetRecord {
                ast_pubkey: Self::encode(key.to_bytes()),
                ast_specification_version: index.specification_version,
                ast_specification_asset_class: index.specification_asset_class,
                ast_royalty_target_type: index.royalty_target_type,
                ast_royalty_amount: index.royalty_amount,
                ast_slot_created: index.slot_created,
                ast_owner_type: index.owner_type,
                ast_owner: index.owner.map(Self::encode),
                ast_delegate: index.delegate.map(Self::encode),
                ast_authority: index.authority.map(Self::encode),
                ast_collection: index.collection.map(Self::encode),
                ast_is_collection_verified: index.is_collection_verified,
                ast_is_burnt: index.is_burnt,
                ast_is_compressible: index.is_compressible,
                ast_is_compressed: index.is_compressed,
                ast_is_frozen: index.is_frozen,
                ast_supply: index.supply,
                ast_metadata_url_id: metadata_url.map(|(k, _)| k).map(Self::encode),
                ast_slot_updated: index.slot_updated,
            };
            assets_writer.serialize(record).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    fn encode<T: AsRef<[u8]>>(v: T) -> String {
        format!("\\x{}", hex::encode(v))
    }
}

#[async_trait]
impl Dumper for Storage {
    async fn dump_db(
        &self,
        base_path: &std::path::Path,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), String> {
        let metadata_path = base_path.join("metadata.csv").to_str().map(str::to_owned);
        if metadata_path.is_none() {
            return Err("invalid path".to_string());
        }
        let creators_path = base_path.join("creators.csv").to_str().map(str::to_owned);
        if creators_path.is_none() {
            return Err("invalid path".to_string());
        }
        let assets_path = base_path.join("assets.csv").to_str().map(str::to_owned);
        if assets_path.is_none() {
            return Err("invalid path".to_string());
        }
        tracing::info!(
            "Dumping to metadata: {:?}, creators: {:?}, assets: {:?}",
            metadata_path,
            creators_path,
            assets_path
        );
        let metadata_file = File::create(metadata_path.unwrap()).unwrap();
        let assets_file = File::create(assets_path.unwrap()).unwrap();
        let creators_file = File::create(creators_path.unwrap()).unwrap();
        // Wrap each file in a BufWriter
        let metadata_buf_writer = BufWriter::with_capacity(ONE_G, metadata_file);
        let assets_buf_writer = BufWriter::with_capacity(ONE_G, assets_file);
        let creators_buf_writer = BufWriter::with_capacity(ONE_G, creators_file);
        let mut metadata_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(metadata_buf_writer);
        let mut assets_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(assets_buf_writer);
        let mut creators_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(creators_buf_writer);

        self.dump_csv(
            &mut metadata_writer,
            &mut creators_writer,
            &mut assets_writer,
            batch_size,
            rx,
        )
        .await?;
        Ok(())
    }
}

impl Storage {
    pub async fn dump_reference_db(
        &self,
        base_path: &std::path::Path,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), String> {
        let asset_path = base_path.join("asset.csv").to_str().map(str::to_owned);
        if asset_path.is_none() {
            return Err("invalid path".to_string());
        }
        let authority_path = base_path
            .join("asset_authority.csv")
            .to_str()
            .map(str::to_owned);
        if authority_path.is_none() {
            return Err("invalid path".to_string());
        }
        let creators_path = base_path
            .join("assets_creators.csv")
            .to_str()
            .map(str::to_owned);
        if creators_path.is_none() {
            return Err("invalid path".to_string());
        }
        let asset_data_path = base_path.join("asset_data.csv").to_str().map(str::to_owned);
        if asset_data_path.is_none() {
            return Err("invalid path".to_string());
        }
        let asset_grouping_path = base_path
            .join("asset_grouping.csv")
            .to_str()
            .map(str::to_owned);
        if asset_grouping_path.is_none() {
            return Err("invalid path".to_string());
        }
        let cl_items_path = base_path.join("cl_items.csv").to_str().map(str::to_owned);
        if cl_items_path.is_none() {
            return Err("invalid path".to_string());
        }
        let cl_audits_v2_path = base_path
            .join("cl_audits_v2.csv")
            .to_str()
            .map(str::to_owned);
        if cl_audits_v2_path.is_none() {
            return Err("invalid path".to_string());
        }
        let token_accounts_path = base_path
            .join("token_accounts.csv")
            .to_str()
            .map(str::to_owned);
        if token_accounts_path.is_none() {
            return Err("invalid path".to_string());
        }
        println!(
            "Dumping to assets: {:?}, authority: {:?}, creators: {:?}, asset_data: {:?}, asset_grouping: {:?}, cl_items: {:?}, cl_audits_v2: {:?}, token_accounts {:?}",
            asset_path,
            authority_path,
            creators_path,
            asset_data_path,
            asset_grouping_path,
            cl_items_path,
            cl_audits_v2_path,
            token_accounts_path
        );
        let asset_file = File::create(asset_path.unwrap()).unwrap();
        let authority_file = File::create(authority_path.unwrap()).unwrap();
        let creators_file = File::create(creators_path.unwrap()).unwrap();
        let asset_data_file = File::create(asset_data_path.unwrap()).unwrap();
        let asset_grouping_file = File::create(asset_grouping_path.unwrap()).unwrap();
        let cl_items_file = File::create(cl_items_path.unwrap()).unwrap();
        let cl_audits_v2_file = File::create(cl_audits_v2_path.unwrap()).unwrap();
        let token_accounts_file = File::create(token_accounts_path.unwrap()).unwrap();

        // Wrap each file in a BufWriter
        let asset_buf_writer = BufWriter::with_capacity(ONE_G, asset_file);
        let authority_buf_writer = BufWriter::with_capacity(ONE_G, authority_file);
        let creators_buf_writer = BufWriter::with_capacity(ONE_G, creators_file);
        let asset_data_buf_writer = BufWriter::with_capacity(ONE_G, asset_data_file);
        let asset_grouping_buf_writer = BufWriter::with_capacity(ONE_G, asset_grouping_file);
        let cl_items_buf_writer = BufWriter::with_capacity(ONE_G, cl_items_file);
        let cl_audits_v2_buf_writer = BufWriter::with_capacity(ONE_G, cl_audits_v2_file);
        let token_accounts_buf_writer = BufWriter::with_capacity(ONE_G, token_accounts_file);

        let mut asset_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(asset_buf_writer);
        let mut authority_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(authority_buf_writer);
        let mut creators_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(creators_buf_writer);
        let mut asset_data_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(asset_data_buf_writer);
        let mut asset_grouping_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(asset_grouping_buf_writer);
        let mut cl_items_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(cl_items_buf_writer);
        let mut cl_audits_v2_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(cl_audits_v2_buf_writer);
        let mut token_accounts_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(token_accounts_buf_writer);

        self.dump_reference_csv(
            &mut asset_writer,
            &mut authority_writer,
            &mut creators_writer,
            &mut asset_data_writer,
            &mut asset_grouping_writer,
            &mut cl_items_writer,
            &mut cl_audits_v2_writer,
            &mut token_accounts_writer,
            batch_size,
            rx,
        )
        .await?;
        Ok(())
    }

    pub async fn dump_reference_csv<W: Write>(
        &self,
        asset_writer: &mut Writer<W>,
        authority_writer: &mut Writer<W>,
        creators_writer: &mut Writer<W>,
        asset_data_writer: &mut Writer<W>,
        asset_grouping_writer: &mut Writer<W>,
        cl_items_writer: &mut Writer<W>,
        cl_audits_v2_writer: &mut Writer<W>,
        token_accounts_writer: &mut Writer<W>,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), String> {
        let iter = self.asset_static_data.iter_start();
        // collect batch of keys
        let mut batch = Vec::with_capacity(batch_size);
        let mut creators_last_id: u64 = 0;
        let mut authority_last_id: u64 = 0;
        let mut grouping_last_id: u64 = 0;
        for k in iter
            .filter_map(|k| k.ok())
            .filter_map(|(key, _)| decode_pubkey(key.to_vec()).ok())
        {
            batch.push(k);
            if batch.len() == batch_size {
                self.dump_reference_csv_batch(
                    &batch,
                    asset_writer,
                    authority_writer,
                    creators_writer,
                    asset_data_writer,
                    asset_grouping_writer,
                    &mut creators_last_id,
                    &mut authority_last_id,
                    &mut grouping_last_id,
                )
                .await?;
                batch.clear();
            }
            if !rx.is_empty() {
                return Err("dump cancelled".to_string());
            }
        }
        if !batch.is_empty() {
            self.dump_reference_csv_batch(
                &batch,
                asset_writer,
                authority_writer,
                creators_writer,
                asset_data_writer,
                asset_grouping_writer,
                &mut creators_last_id,
                &mut authority_last_id,
                &mut grouping_last_id,
            )
            .await?;
        }
        let iter = self.token_accounts.iter_start();
        for token_account in iter
            .filter_map(|k| k.ok())
            .flat_map(|(_, value)| deserialize::<TokenAccount>(value.as_ref()).ok())
        {
            token_accounts_writer
                .serialize((
                    Self::encode(token_account.pubkey.to_bytes()),
                    Self::encode(token_account.mint.to_bytes()),
                    token_account.amount,
                    Self::encode(token_account.owner.to_bytes()),
                    token_account.frozen,
                    Self::encode(token_account.owner.to_bytes()),
                    token_account
                        .delegate
                        .map(|delegate| Self::encode(delegate.to_bytes())),
                    token_account.delegated_amount,
                    token_account.slot_updated,
                    Self::encode(
                        Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
                            .unwrap()
                            .to_bytes(),
                    ),
                ))
                .map_err(|e| e.to_string())?;
        }
        let mut cl_items_last_id = 0;
        let iter = self.cl_items.iter_start();
        for cl_item in iter
            .filter_map(|k| k.ok())
            .flat_map(|(_, value)| deserialize::<ClItem>(value.as_ref()).ok())
        {
            cl_items_writer
                .serialize((
                    cl_items_last_id,
                    Self::encode(cl_item.cli_tree_key),
                    cl_item.cli_node_idx,
                    cl_item.cli_leaf_idx,
                    cl_item.cli_seq,
                    cl_item.cli_level,
                    Self::encode(cl_item.cli_hash),
                ))
                .map_err(|e| e.to_string())?;
            cl_items_last_id += 1;
        }

        let mut cl_audit_last_id = 0;
        let iter = self.asset_signature.iter_start();
        for (asset_signature_key, asset_signature) in
            iter.filter_map(|k| k.ok()).flat_map(|(key, value)| {
                let asset_signature = deserialize::<AssetSignature>(value.as_ref()).ok()?;
                let asset_signature_key = AssetSignature::decode_key(key.as_ref().to_vec()).ok()?;
                Some((asset_signature_key, asset_signature))
            })
        {
            cl_audits_v2_writer
                .serialize((
                    cl_audit_last_id,
                    Self::encode(asset_signature_key.tree),
                    asset_signature_key.leaf_idx,
                    asset_signature_key.seq,
                    Utc::now(),
                    Self::encode(
                        Signature::from_str(&asset_signature.tx).map_err(|e| e.to_string())?,
                    ),
                    asset_signature.instruction,
                ))
                .map_err(|e| e.to_string())?;
            cl_audit_last_id += 1;
        }

        asset_writer.flush().map_err(|e| e.to_string())?;
        authority_writer.flush().map_err(|e| e.to_string())?;
        creators_writer.flush().map_err(|e| e.to_string())?;
        asset_data_writer.flush().map_err(|e| e.to_string())?;
        asset_grouping_writer.flush().map_err(|e| e.to_string())?;
        cl_items_writer.flush().map_err(|e| e.to_string())?;
        cl_audits_v2_writer.flush().map_err(|e| e.to_string())?;
        token_accounts_writer.flush().map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn dump_reference_csv_batch<W: Write>(
        &self,
        batch: &[Pubkey],
        asset_writer: &mut Writer<W>,
        authority_writer: &mut Writer<W>,
        creators_writer: &mut Writer<W>,
        asset_data_writer: &mut Writer<W>,
        asset_grouping_writer: &mut Writer<W>,
        creators_last_id: &mut u64,
        authority_last_id: &mut u64,
        grouping_last_id: &mut u64,
    ) -> Result<(), String> {
        let indexes = self
            .get_reference_asset_indexes(batch)
            .await
            .map_err(|e| e.to_string())?;
        for (key, index) in indexes {
            let asset = ReferenceAssetRecord {
                ast_pubkey: Self::encode(key.to_bytes()),
                alt_id: Self::encode(key.to_bytes()),
                specification_version: index.specification_version,
                specification_asset_class: index.specification_asset_class,
                owner: index.owner.map(Self::encode),
                owner_type: index.owner_type,
                delegate: index.delegate.map(Self::encode),
                frozen: index.is_frozen,
                supply: index.supply,
                supply_mint: Self::encode(key.to_bytes()),
                compressed: index.is_compressed,
                compressible: index.is_compressible,
                seq: index.slot_updated,
                tree_id: index.tree.map(|tree| Self::encode(tree.to_bytes())),
                leaf: index.leaf.map(Self::encode),
                nonce: index.nonce,
                royalty_target_type: index.royalty_target_type,
                royalty_target: Some(Self::encode(key.to_bytes())), // royalty_target
                royalty_amount: index.royalty_amount,
                asset_data: Some(Self::encode(key.to_bytes())),
                created_at: Utc::now(),
                data_hash: index
                    .data_hash
                    .map(|data_hash| Self::encode(data_hash.to_bytes())),
                creator_hash: index
                    .creator_hash
                    .map(|creator_hash| Self::encode(creator_hash.to_bytes())),
                owner_delegate_seq: index.slot_updated, // owner_delegate_seq
                leaf_seq: index.slot_updated,           // leaf_seq
                base_info_seq: index.slot_updated,      // base_info_seq
                slot_updated_metadata_account: index.slot_updated, // slot_updated_metadata
                slot_updated_token_account: index.slot_updated, // slot_updated_token
                slot_updated_mint_account: index.slot_updated, // slot_updated_mint
                slot_updated_cnft_transaction: index.slot_updated, // slot_updated_cnft
                mpl_core_plugins: index.mpl_core_plugins,
                mpl_core_unknown_plugins: index.mpl_core_unknown_plugins,
                mpl_core_collection_num_minted: index.mpl_core_collection_num_minted,
                mpl_core_collection_current_size: index.mpl_core_collection_current_size,
                mpl_core_plugins_json_version: index.mpl_core_plugins_json_version,
            };
            asset_writer.serialize(asset).map_err(|e| e.to_string())?;
            if let Some(authority) = index.authority {
                authority_writer
                    .serialize((
                        &authority_last_id,
                        Self::encode(key.to_bytes()),
                        "",
                        Self::encode(authority.to_bytes()),
                        index.slot_updated,
                        index.slot_updated,
                    ))
                    .map_err(|e| e.to_string())?;
            }

            authority_last_id.add_assign(1);
            for (position, creator) in index.creators.iter().enumerate() {
                creators_writer
                    .serialize((
                        &creators_last_id,
                        Self::encode(key.to_bytes()),
                        Self::encode(creator.creator),
                        creator.creator_share,
                        creator.creator_verified,
                        index.slot_updated,
                        position,
                    ))
                    .map_err(|e| e.to_string())?;
                creators_last_id.add_assign(1);
            }

            asset_data_writer
                .serialize((
                    Self::encode(key.to_bytes()),
                    index.slot_updated, // chain_data_mutability
                    index.slot_updated, // chain_data
                    index.metadata_url.map(|m| m.metadata_url),
                    index.slot_updated, // metadata_mutability
                    index.slot_updated, // metadata
                    index.slot_updated,
                    false,
                    index.slot_updated, // raw_name
                    index.slot_updated, // raw_symbol
                    index.slot_updated, // base_info_seq
                ))
                .map_err(|e| e.to_string())?;
            if let Some(collection) = index.collection {
                asset_grouping_writer
                    .serialize((
                        &grouping_last_id,
                        Self::encode(key.to_bytes()),
                        "collection",
                        collection.to_string(),
                        index.slot_updated,
                        index.slot_updated,
                        index.is_collection_verified.unwrap_or_default(),
                        index.slot_updated,
                    ))
                    .map_err(|e| e.to_string())?;
                grouping_last_id.add_assign(1);
            }
        }
        Ok(())
    }
}
