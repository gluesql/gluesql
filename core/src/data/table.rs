use {
    crate::{
        ast::{IndexItem, TableAlias, TableFactor},
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

pub fn get_alias(table_factor: &TableFactor) -> Result<&String> {
    match table_factor {
        TableFactor::Table {
            name, alias: None, ..
        } => Ok(name),
        TableFactor::Table {
            alias: Some(TableAlias { name, .. }),
            ..
        }
        | TableFactor::Derived {
            alias: TableAlias { name, .. },
            ..
        } => Ok(name),
        TableFactor::Series {
            name, alias: None, ..
        } => Ok(name),
        TableFactor::Series {
            alias: Some(TableAlias { name, .. }),
            ..
        } => Ok(name),
    }
}

pub fn get_index(table_factor: &TableFactor) -> Option<&IndexItem> {
    match table_factor {
        TableFactor::Table { index, .. } => index.as_ref(),
        TableFactor::Derived { .. } => None,
        TableFactor::Series { .. } => None,
    }
}
