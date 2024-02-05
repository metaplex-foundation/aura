use std::sync::Mutex;

use solana_sdk::pubkey::Pubkey;

pub mod api_req_params;
pub mod enums;
pub mod models;
#[macro_use]
extern crate lazy_static;

lazy_static! {
    pub static ref TARGET_PUBKEY: Mutex<Option<Pubkey>> = Mutex::new(
        Some(Pubkey::try_from("BhaxAEHxhCgwt2vUyF1aNQsPTwH7rStVqhcfePWdUz9A").unwrap())
        // Pubkey::from([0;32])
    );
}
