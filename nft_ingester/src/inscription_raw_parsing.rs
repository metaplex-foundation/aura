use crate::error::IngesterError;
use anchor_lang::{AccountDeserialize, Discriminator};
use libreplex_inscriptions::{
    Inscription, InscriptionRankPage, InscriptionSummary, InscriptionV3, Migrator,
};

pub enum ParsedInscription {
    Inscription(Inscription),
    InscriptionData(Vec<u8>),
    UnhandledAccount,
}

pub fn handle_inscription_account(
    mut account_data: &[u8],
) -> Result<ParsedInscription, IngesterError> {
    // If data len less than DISCRIMINATOR len it means
    // that account data was rewrote by raw data of InscriptionData
    // account type
    if account_data.len() < 8 {
        return Ok(ParsedInscription::InscriptionData(account_data.to_vec()));
    }
    let mut discriminator = [0u8; 8];
    discriminator.copy_from_slice(&account_data[..8]);

    let res = match discriminator {
        Inscription::DISCRIMINATOR => {
            let inscription = Inscription::try_deserialize(&mut account_data)?;
            ParsedInscription::Inscription(inscription)
        }
        // no need for indexing such accounts
        InscriptionRankPage::DISCRIMINATOR
        | InscriptionSummary::DISCRIMINATOR
        | InscriptionV3::DISCRIMINATOR
        | Migrator::DISCRIMINATOR => ParsedInscription::UnhandledAccount,
        // InscriptionData account does not contain DISCRIMINATOR because it is overwritten by binary data
        // so we consider InscriptionData account to be the one not matching any other account types
        _ => ParsedInscription::InscriptionData(account_data.to_vec()),
    };

    Ok(res)
}
