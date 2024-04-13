use gluesql_core::ast::Expr;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ColumnDescription {
    pub default: Option<Expr>,
    pub comment: Option<String>,
}
