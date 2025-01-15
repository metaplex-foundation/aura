use std::str::FromStr;

use chrono::{DateTime, Utc};
use flatbuffers::FlatBufferBuilder;
use solana_sdk::pubkey::Pubkey;

use crate::error::{IngesterError, IngesterError::MissingFlatbuffersFieldError};

#[derive(Clone)]
pub struct FlatbufferMapper {}

impl FlatbufferMapper {
    pub fn map_tx_fb_bytes(
        &self,
        tx: utils::flatbuffer::transaction_info_generated::transaction_info::TransactionInfo,
        seen_at: DateTime<Utc>,
    ) -> Result<Vec<u8>, IngesterError> {
        let mut builder = FlatBufferBuilder::new();

        let slot_idx = format!("{}-{}", tx.slot(), tx.index().unwrap_or_default());
        let signature =
            tx.signature_string().ok_or(MissingFlatbuffersFieldError("signature".to_string()))?;

        let versioned_tx = bincode::deserialize::<solana_sdk::transaction::VersionedTransaction>(
            tx.transaction()
                .ok_or(MissingFlatbuffersFieldError("transaction".to_string()))?
                .bytes(),
        )?;
        let version = match versioned_tx.message {
            solana_sdk::message::VersionedMessage::Legacy(_) => {
                plerkle_serialization::TransactionVersion::Legacy
            },
            solana_sdk::message::VersionedMessage::V0(_) => {
                plerkle_serialization::TransactionVersion::V0
            },
        };

        let mut all_tx_keys = Vec::new();
        for keys in &[
            tx.account_keys_string(),
            tx.loaded_addresses_string().and_then(|loaded_addresses| loaded_addresses.writable()),
            tx.loaded_addresses_string().and_then(|loaded_addresses| loaded_addresses.readonly()),
        ] {
            let account_keys = keys
                .map(|keys| {
                    keys.iter()
                        .map(|key| {
                            Ok(plerkle_serialization::Pubkey::new(
                                &Pubkey::from_str(key)?.to_bytes(),
                            ))
                        })
                        .collect::<Result<Vec<plerkle_serialization::Pubkey>, IngesterError>>()
                })
                .transpose()?;

            if let Some(keys) = account_keys {
                all_tx_keys.extend(keys);
            }
        }
        let account_keys = if all_tx_keys.is_empty() {
            None
        } else {
            Some(builder.create_vector(all_tx_keys.as_slice()))
        };

        let log_messages = tx.transaction_meta().and_then(|meta| meta.log_messages()).map(|msgs| {
            let mapped = msgs.iter().map(|msg| builder.create_string(msg)).collect::<Vec<_>>();
            builder.create_vector(&mapped)
        });

        let outer_instructions = versioned_tx.message.instructions();
        let outer_instructions = if !outer_instructions.is_empty() {
            let mut instructions_fb_vec = Vec::with_capacity(outer_instructions.len());
            for compiled_instruction in outer_instructions.iter() {
                let program_id_index = compiled_instruction.program_id_index;
                let accounts = Some(builder.create_vector(&compiled_instruction.accounts));
                let data = Some(builder.create_vector(&compiled_instruction.data));
                instructions_fb_vec.push(plerkle_serialization::CompiledInstruction::create(
                    &mut builder,
                    &plerkle_serialization::CompiledInstructionArgs {
                        program_id_index,
                        accounts,
                        data,
                    },
                ));
            }

            Some(builder.create_vector(&instructions_fb_vec))
        } else {
            None
        };

        let inner_instructions = if let Some(inner_instructions_vec) = tx.inner_instructions() {
            let mut overall_fb_vec = Vec::with_capacity(inner_instructions_vec.len());
            for inner_instructions in inner_instructions_vec.iter() {
                let index = inner_instructions.index();
                if let Some(instructions) = inner_instructions.instructions() {
                    let mut instructions_fb_vec = Vec::with_capacity(instructions.len());
                    for inner_instruction_v2 in instructions.iter() {
                        let compiled_instruction = match inner_instruction_v2.instruction() {
                            Some(instruction) => instruction,
                            None => continue,
                        };

                        let program_id_index = compiled_instruction.program_id_index();
                        let accounts = compiled_instruction
                            .accounts()
                            .map(|acc| builder.create_vector(acc.bytes()));
                        let data = compiled_instruction
                            .data()
                            .map(|data| builder.create_vector(data.bytes()));
                        let compiled = plerkle_serialization::CompiledInstruction::create(
                            &mut builder,
                            &plerkle_serialization::CompiledInstructionArgs {
                                program_id_index,
                                accounts,
                                data,
                            },
                        );
                        instructions_fb_vec.push(
                            plerkle_serialization::CompiledInnerInstruction::create(
                                &mut builder,
                                &plerkle_serialization::CompiledInnerInstructionArgs {
                                    compiled_instruction: Some(compiled),
                                    stack_height: 0, // Desperatley need this when it comes in 1.15
                                },
                            ),
                        );
                    }

                    let instructions = Some(builder.create_vector(&instructions_fb_vec));
                    overall_fb_vec.push(plerkle_serialization::CompiledInnerInstructions::create(
                        &mut builder,
                        &plerkle_serialization::CompiledInnerInstructionsArgs {
                            index,
                            instructions,
                        },
                    ))
                }
            }

            Some(builder.create_vector(&overall_fb_vec))
        } else {
            None
        };

        let args = plerkle_serialization::TransactionInfoArgs {
            is_vote: tx.is_vote(),
            account_keys,
            log_messages,
            inner_instructions: None,
            outer_instructions,
            slot: tx.slot(),
            slot_index: Some(builder.create_string(&slot_idx)),
            seen_at: seen_at.timestamp_millis(),
            signature: Some(builder.create_string(signature)),
            compiled_inner_instructions: inner_instructions,
            version,
        };
        let transaction_info_wip =
            plerkle_serialization::TransactionInfo::create(&mut builder, &args);
        builder.finish(transaction_info_wip, None);

        Ok(builder.finished_data().to_owned())
    }
}
