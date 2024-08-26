use anchor_lang::Discriminator;
use anchor_lang::AccountDeserialize;
use crate::error::IngesterError;
use libreplex_inscriptions::{Inscription, InscriptionData};

pub enum ParsedInscription {
    Inscription(Inscription),
    InscriptionData(InscriptionData),
    EmptyAccount,
}

pub fn handle_inscription_account(account_data: &[u8]) -> Result<ParsedInscription, IngesterError> {
    if account_data.len() < 8 {
        return Err(IngesterError::AccountParsingError("".to_string()));
    }
    let discriminator = account_data[..8];
    let res = match discriminator {
        &Inscription::DISCRIMINATOR => {
            let inscription = Inscription::try_deserialize(&mut account_data.clone())?;
            ParsedInscription::Inscription(inscription)
        }
        &InscriptionData::DISCRIMINATOR => {}
    };

    Ok(res)
}
