use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone)]
pub struct Body {
    pub jsonrpc: String,
    pub id: usize,
    pub method: String,
    pub params: Value,
}

impl Body {
    pub fn new(method: &str, params: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_owned(),
            id: 0,
            method: method.to_owned(),
            params,
        }
    }
}
