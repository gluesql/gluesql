use {
    gluesql_core::ast::{Expr, ForeignKey},
    serde::{Deserialize, Serialize},
};

#[derive(Serialize, Deserialize)]
pub struct TableDescription {
    pub foreign_keys: Vec<ForeignKey>,
    pub comment: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ColumnDescription {
    pub default: Option<Expr>,
    pub comment: Option<String>,
}
