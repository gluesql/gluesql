use {
    crate::{
        ast::{ObjectName, TableAlias, TableFactor},
        result::Result,
    },
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum TableError {
    #[error("unreachable")]
    Unreachable,
}

pub fn get_name(table_name: &ObjectName) -> Result<&String> {
    let ObjectName(idents) = table_name;
    idents.last().ok_or_else(|| TableError::Unreachable.into())
}

pub fn get_alias(table_factor: &TableFactor) -> Result<&String> {
    match table_factor {
        TableFactor::Table {
            name, alias: None, ..
        } => get_name(name),
        TableFactor::Table {
            alias: Some(TableAlias { name, .. }),
            ..
        }
        | TableFactor::Derived {
            alias: TableAlias { name, .. },
            ..
        } => Ok(name),
    }
}

#[cfg(feature = "index")]
use crate::ast::IndexItem;
#[cfg(feature = "index")]
pub fn get_index(table_factor: &TableFactor) -> Option<&IndexItem> {
    match table_factor {
        TableFactor::Table { index, .. } => index.as_ref(),
        TableFactor::Derived { .. } => None,
    }
}
