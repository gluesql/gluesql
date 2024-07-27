use {
    gluesql_core::{ast::{Expr, ForeignKey, UniqueConstraint}, data::Trigger},
    serde::{Deserialize, Serialize}, std::collections::HashMap,
};

#[derive(Serialize, Deserialize)]
pub struct TableDescription {
    pub foreign_keys: Vec<ForeignKey>,
    pub primary_key: Option<Vec<usize>>,
    pub unique_constraints: Vec<UniqueConstraint>,
    pub comment: Option<String>,
    pub triggers: HashMap<String, Trigger>
}

#[derive(Serialize, Deserialize)]
pub struct ColumnDescription {
    pub default: Option<Expr>,
    pub comment: Option<String>,
}
