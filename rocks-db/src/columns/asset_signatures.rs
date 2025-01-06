use crate::column::TypedColumn;
use crate::key_encoders::{decode_asset_signature_key, encode_asset_signature_key};
use crate::{Result, Storage};
use entities::api_req_params::AssetSortDirection;
use entities::models::{AssetSignature, AssetSignatureKey, AssetSignatureWithPagination};
use interface::asset_sigratures::AssetSignaturesGetter;
use solana_sdk::pubkey::Pubkey;

impl TypedColumn for AssetSignature {
    type KeyType = AssetSignatureKey;
    type ValueType = Self;
    // The value type is the Asset struct itself
    const NAME: &'static str = "ASSET_SIGNATURE"; // Name of the column family

    fn encode_key(key: AssetSignatureKey) -> Vec<u8> {
        encode_asset_signature_key(key)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_asset_signature_key(bytes)
    }
}

impl AssetSignaturesGetter for Storage {
    fn signatures_iter(
        &self,
        tree: Pubkey,
        leaf_idx: u64,
        page: Option<u64>,
        before_sequence: Option<u64>,
        after_sequence: Option<u64>,
        direction: &AssetSortDirection,
        limit: u64,
    ) -> impl Iterator<Item = (AssetSignatureKey, AssetSignature)> {
        let iter = match direction {
            AssetSortDirection::Asc => self.asset_signature.iter(AssetSignatureKey {
                tree,
                leaf_idx,
                // Skip first elem if after is_some
                seq: after_sequence
                    .map(|seq| seq.saturating_add(1))
                    .unwrap_or_default(),
            }),
            AssetSortDirection::Desc => self.asset_signature.iter_reverse(AssetSignatureKey {
                tree,
                leaf_idx,
                // Skip first elem if after is_some
                seq: after_sequence
                    .map(|seq| seq.saturating_sub(1))
                    .unwrap_or(u64::MAX),
            }),
        };

        iter.skip(
            page.and_then(|page| page.saturating_sub(1).checked_mul(limit))
                .unwrap_or_default() as usize,
        )
        .filter_map(std::result::Result::ok)
        .map_while(move |(key, value)| {
            let key = self.asset_signature.decode_key(key.to_vec()).ok()?;
            if key.tree != tree
                || key.leaf_idx != leaf_idx
                || before_sequence
                    .map(|before_sequence| {
                        before_sequence <= key.seq && matches!(direction, AssetSortDirection::Asc)
                            || before_sequence >= key.seq
                                && matches!(direction, AssetSortDirection::Desc)
                    })
                    .unwrap_or_default()
            {
                return None;
            }
            let value = bincode::deserialize::<AssetSignature>(value.as_ref()).ok()?;
            Some((key, value))
        })
    }

    fn get_asset_signatures(
        &self,
        tree: Pubkey,
        leaf_idx: u64,
        before_sequence: Option<u64>,
        after_sequence: Option<u64>,
        page: Option<u64>,
        direction: AssetSortDirection,
        limit: u64,
    ) -> AssetSignatureWithPagination {
        let mut res = AssetSignatureWithPagination::default();
        let mut first_iter = true;
        for (key, value) in self.signatures_iter(
            tree,
            leaf_idx,
            page,
            before_sequence,
            after_sequence,
            &direction,
            limit,
        ) {
            if res.asset_signatures.len() >= limit as usize {
                break;
            }
            res.asset_signatures.push(value);
            res.after = Some(key.seq);
            if first_iter {
                res.before = Some(key.seq);
                first_iter = false;
            }
        }
        res
    }
}
