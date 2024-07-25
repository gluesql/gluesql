use {
    gluesql_core::ast::{Expr, ForeignKey, UniqueConstraint},
    serde::{Deserialize, Serialize},
};

#[derive(Serialize, Deserialize)]
pub struct TableDescription {
    pub foreign_keys: Vec<ForeignKey>,
    pub primary_key: Option<Vec<usize>>,
    pub unique_constraints: Vec<UniqueConstraint>,
    pub comment: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ColumnDescription {
    pub default: Option<Expr>,
    pub comment: Option<String>,
}
