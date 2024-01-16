use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone)]
pub struct Body {
    pub(crate) jsonrpc: String,
    pub(crate) id: String,
    pub(crate) method: String,
    pub(crate) params: String,
}

impl Body {
    fn new(method: &str, params: &str) -> Self {
        Self {
            jsonrpc: "2.0".to_owned(),
            id: "string".to_owned(),
            method: method.to_owned(),
            params: params.to_owned(),
        }
    }
}
