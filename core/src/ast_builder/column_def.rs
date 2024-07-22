use crate::{
    ast::ColumnDef,
    parse_sql::parse_column_def,
    result::{Error, Result},
    sqlparser::ast::ColumnDef as SqlColumnDef,
    translate::translate_column_def,
};

#[derive(Clone, Debug)]
pub struct PrimaryKeyConstraintNode {
    columns: Vec<String>,
}

impl<I: IntoIterator<Item = S>, S: ToString> From<I> for PrimaryKeyConstraintNode {
    fn from(columns: I) -> Self {
        PrimaryKeyConstraintNode {
            columns: columns
                .into_iter()
                .map(|column| column.to_string())
                .collect(),
        }
    }
}

impl AsRef<[String]> for PrimaryKeyConstraintNode {
    fn as_ref(&self) -> &[String] {
        &self.columns
    }
}

impl From<PrimaryKeyConstraintNode> for Vec<String> {
    fn from(primary_key_constraint: PrimaryKeyConstraintNode) -> Vec<String> {
        primary_key_constraint.columns
    }
}

#[derive(Clone, Debug)]
pub enum ColumnDefNode {
    Text(String),
}

impl AsRef<str> for ColumnDefNode {
    fn as_ref(&self) -> &str {
        match self {
            ColumnDefNode::Text(column_def) => column_def,
        }
    }
}

impl From<&str> for ColumnDefNode {
    fn from(column_def: &str) -> Self {
        ColumnDefNode::Text(column_def.to_owned())
    }
}

impl TryFrom<ColumnDefNode> for SqlColumnDef {
    type Error = Error;

    fn try_from(column_def_node: ColumnDefNode) -> Result<SqlColumnDef> {
        match column_def_node {
            ColumnDefNode::Text(column_def) => parse_column_def(column_def),
        }
    }
}

impl TryFrom<ColumnDefNode> for ColumnDef {
    type Error = Error;

    fn try_from(column_def_node: ColumnDefNode) -> Result<ColumnDef> {
        let sql_column_definition: SqlColumnDef = column_def_node.try_into()?;
        translate_column_def(&sql_column_definition)
    }
}
