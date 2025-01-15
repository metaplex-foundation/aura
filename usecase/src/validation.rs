use std::str::FromStr;

use interface::error::UsecaseError;
use solana_sdk::pubkey::Pubkey;

pub fn validate_pubkey(str_pubkey: String) -> Result<Pubkey, UsecaseError> {
    Pubkey::from_str(&str_pubkey).map_err(|_| UsecaseError::PubkeyValidationError(str_pubkey))
}

pub fn validate_opt_pubkey_vec(pubkey: &Option<String>) -> Result<Option<Vec<u8>>, UsecaseError> {
    let opt_bytes = if let Some(pubkey) = pubkey {
        let pubkey = Pubkey::from_str(pubkey)
            .map_err(|_| UsecaseError::PubkeyValidationError(pubkey.to_string()))?;
        Some(pubkey.to_bytes().to_vec())
    } else {
        None
    };
    Ok(opt_bytes)
}

pub fn validate_opt_pubkey(pubkey: &Option<String>) -> Result<Option<Pubkey>, UsecaseError> {
    pubkey
        .as_ref()
        .map(|pubkey| {
            Pubkey::from_str(pubkey)
                .map_err(|_| UsecaseError::PubkeyValidationError(pubkey.to_string()))
        })
        .transpose()
}
