use std::str::FromStr;

use base64::Engine;
use solana_sdk::{
    bs58, instruction::CompiledInstruction, message::v0::LoadedAddresses, pubkey::Pubkey,
    transaction_context::TransactionReturnData,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransactionWithStatusMeta, InnerInstruction,
    InnerInstructions, TransactionStatusMeta, TransactionTokenBalance, TransactionWithStatusMeta,
    UiInstruction, UiTransactionTokenBalance, VersionedTransactionWithStatusMeta,
};

pub fn decode_encoded_transaction_with_status_meta(
    t: EncodedTransactionWithStatusMeta,
) -> Option<TransactionWithStatusMeta> {
    fn get<T>(s: OptionSerializer<T>) -> Option<T> {
        s.into()
    }
    match t.meta {
        Some(m) => {
            let inner_instructions = get(m.inner_instructions).map(|ii| {
                ii.into_iter()
                    .map(|ii| InnerInstructions {
                        index: ii.index,
                        instructions: ii
                            .instructions
                            .into_iter()
                            .filter_map(|ui| {
                                if let UiInstruction::Compiled(i) = ui {
                                    Some(InnerInstruction {
                                        instruction: CompiledInstruction {
                                            program_id_index: i.program_id_index,
                                            accounts: i.accounts,
                                            data: bs58::decode(i.data).into_vec().ok()?,
                                        },
                                        stack_height: i.stack_height,
                                    })
                                } else {
                                    None
                                }
                            })
                            .collect(),
                    })
                    .collect()
            });
            let meta = TransactionStatusMeta {
                status: m.status,
                fee: m.fee,
                pre_balances: m.pre_balances,
                post_balances: m.post_balances,
                inner_instructions,
                log_messages: get(m.log_messages),
                pre_token_balances: get(m.pre_token_balances)
                    .map(decode_transaction_token_balances),
                post_token_balances: get(m.post_token_balances)
                    .map(decode_transaction_token_balances),
                rewards: m.rewards.into(),
                loaded_addresses: get(m.loaded_addresses)
                    .map(|ula| LoadedAddresses {
                        writable: ula
                            .writable
                            .into_iter()
                            .filter_map(|a| Pubkey::from_str(&a).ok())
                            .collect(),
                        readonly: ula
                            .readonly
                            .into_iter()
                            .filter_map(|a| Pubkey::from_str(&a).ok())
                            .collect(),
                    })
                    .unwrap_or_default(),
                return_data: get(m.return_data).and_then(|urd| {
                    Some(TransactionReturnData {
                        program_id: Pubkey::from_str(&urd.program_id).ok()?,
                        data: base64::prelude::BASE64_STANDARD.decode(urd.data.0).ok()?,
                    })
                }),
                compute_units_consumed: m.compute_units_consumed.into(),
            };
            let versioned_txn_with_status_meta =
                VersionedTransactionWithStatusMeta { transaction: t.transaction.decode()?, meta };
            let transaction_with_status_meta =
                TransactionWithStatusMeta::Complete(versioned_txn_with_status_meta);
            Some(transaction_with_status_meta)
        },
        None => None,
    }
}

fn decode_transaction_token_balances(
    ui_transaction_token_balances: Vec<UiTransactionTokenBalance>,
) -> Vec<TransactionTokenBalance> {
    ui_transaction_token_balances
        .into_iter()
        .map(|uttb| TransactionTokenBalance {
            account_index: uttb.account_index,
            mint: uttb.mint,
            ui_token_amount: uttb.ui_token_amount,
            owner: Option::<String>::from(uttb.owner).unwrap_or_default(),
            program_id: Option::<String>::from(uttb.program_id).unwrap_or_default(),
        })
        .collect()
}
