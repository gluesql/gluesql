use gluesql_core::ast::ForeignKey;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct TableDescription {
    pub foreign_keys: Option<Vec<ForeignKey>>,
}
