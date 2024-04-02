//! SeaORM Entity. Generated by sea-orm-codegen 0.9.3

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Default, Debug)]
pub struct Entity;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Model {
    pub id: i64,
    pub asset_id: Vec<u8>,
    pub creator: Vec<u8>,
    pub share: i32,
    pub verified: bool,
    pub seq: Option<i64>,
    pub slot_updated: Option<i64>,
    pub position: i16,
}

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Column {
    Id,
    AssetId,
    Creator,
    Share,
    Verified,
    Seq,
    SlotUpdated,
    Position,
}
