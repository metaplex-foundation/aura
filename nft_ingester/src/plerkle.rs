use plerkle_serialization::deserializer::*;
use solana_program::instruction::CompiledInstruction;
use solana_program::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::InnerInstructions;

#[derive(thiserror::Error, Clone, Debug, PartialEq)]
pub enum PlerkleDeserializerError {
    #[error("Not found")]
    NotFound,
    #[error("Solana error: {0}")]
    Solana(#[from] SolanaDeserializerError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountInfo {
    pub slot: u64,
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub lamports: u64,
    pub rent_epoch: u64,
    pub executable: bool,
    pub write_version: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionInfo {
    pub slot: u64,
    pub signature: Signature,
    pub account_keys: Vec<Pubkey>,
    pub message_instructions: Vec<CompiledInstruction>,
    pub meta_inner_instructions: Vec<InnerInstructions>,
}

pub struct PlerkleAccountInfo<'a>(pub plerkle_serialization::AccountInfo<'a>);

impl TryFrom<PlerkleAccountInfo<'_>> for AccountInfo {
    type Error = PlerkleDeserializerError;

    fn try_from(value: PlerkleAccountInfo) -> Result<Self, Self::Error> {
        let account_info = value.0;

        Ok(Self {
            slot: account_info.slot(),
            pubkey: account_info
                .pubkey()
                .ok_or(PlerkleDeserializerError::NotFound)?
                .try_into()?,
            owner: account_info
                .owner()
                .ok_or(PlerkleDeserializerError::NotFound)?
                .try_into()?,
            lamports: account_info.lamports(),
            rent_epoch: account_info.rent_epoch(),
            executable: account_info.executable(),
            write_version: account_info.write_version(),
            data: PlerkleOptionalU8Vector(account_info.data()).try_into()?,
        })
    }
}

pub struct PlerkleTransactionInfo<'a>(pub plerkle_serialization::TransactionInfo<'a>);

impl<'a> TryFrom<PlerkleTransactionInfo<'a>> for TransactionInfo {
    type Error = PlerkleDeserializerError;

    fn try_from(value: PlerkleTransactionInfo<'a>) -> Result<Self, Self::Error> {
        let tx_info = value.0;

        let slot = tx_info.slot();
        let signature = PlerkleOptionalStr(tx_info.signature()).try_into()?;
        let account_keys = PlerkleOptionalPubkeyVector(tx_info.account_keys()).try_into()?;
        let message_instructions = PlerkleCompiledInstructionVector(
            tx_info
                .outer_instructions()
                .ok_or(PlerkleDeserializerError::NotFound)?,
        )
        .try_into()?;
        let compiled = tx_info.compiled_inner_instructions();
        let inner = tx_info.inner_instructions();
        let meta_inner_instructions = if let Some(c) = compiled {
            PlerkleCompiledInnerInstructionVector(c).try_into()
        } else {
            PlerkleInnerInstructionsVector(inner.ok_or(PlerkleDeserializerError::NotFound)?)
                .try_into()
        }?;

        Ok(Self {
            slot,
            signature,
            account_keys,
            message_instructions,
            meta_inner_instructions,
        })
    }
}
