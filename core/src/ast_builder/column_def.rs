//! AST Builder node for column definitions
use crate::{
    ast::{CheckConstraint, ColumnDef},
    parse_sql::parse_column_def,
    result::Result,
    translate::translate_column_def,
};

#[derive(Clone, Debug)]
pub enum ColumnDefNode {
    Text(String),
}

impl ColumnDefNode {
    pub fn parse(self) -> Result<(ColumnDef, Option<CheckConstraint>)> {
        match self {
            ColumnDefNode::Text(text) => {
                parse_column_def(text).and_then(|column_def| translate_column_def(&column_def))
            }
        }
    }
}

impl From<&str> for ColumnDefNode {
    fn from(column_def: &str) -> Self {
        ColumnDefNode::Text(column_def.to_owned())
    }
}
