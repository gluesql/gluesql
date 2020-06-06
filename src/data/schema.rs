use serde::{Deserialize, Serialize};
use sqlparser::ast::ColumnDef;

#[derive(Serialize, Deserialize)]
pub struct Schema {
    pub table_name: String,
    pub column_defs: Vec<ColumnDef>,
}
