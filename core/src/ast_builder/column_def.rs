use crate::{
    ast::ColumnDef,
    parse_sql::parse_column_def,
    result::{Error, Result},
    translate::translate_column_def,
};

#[derive(Clone)]
pub enum ColumnDefNode {
    Text(String),
}

impl TryFrom<ColumnDefNode> for ColumnDef {
    type Error = Error;

    fn try_from(column_def_node: ColumnDefNode) -> Result<ColumnDef> {
        match column_def_node {
            ColumnDefNode::Text(column_def) => parse_column_def(column_def)
                .and_then(|column_def| translate_column_def(&column_def)),
        }
    }
}

impl From<&str> for ColumnDefNode {
    fn from(column_def: &str) -> Self {
        ColumnDefNode::Text(column_def.to_owned())
    }
}
