use {
    gluesql_core::ast::{Expr, ForeignKey},
    serde::{Deserialize, Serialize},
};

#[derive(Serialize, Deserialize)]
pub struct TableDescription {
    pub foreign_keys: Option<Vec<ForeignKey>>,
}

#[derive(Serialize, Deserialize)]
pub struct ColumnDescription {
    pub default: Option<Expr>,
    pub comment: Option<String>,
}