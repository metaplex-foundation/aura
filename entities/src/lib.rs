use std::sync::Mutex;

use solana_program::pubkey::Pubkey;

pub mod api_req_params;
pub mod enums;
pub mod models;
#[macro_use]
extern crate lazy_static;

lazy_static! {
    pub static ref TARGET_PUBKEY: Mutex<Pubkey> = Mutex::new(
        Pubkey::from_str("13zxXAoAKpBwjfd7WxssYXn5Ge2nAomr2pXP9sY9M1EE").unwrap()
        // Pubkey::from([0;32])
    );
}