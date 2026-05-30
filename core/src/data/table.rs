use {
    crate::ast::{TableAlias, TableFactor},
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum TableError {
    #[error("unreachable")]
    Unreachable,
}

pub fn get_alias(table_factor: &TableFactor) -> &String {
    match table_factor {
        TableFactor::Table {
            name, alias: None, ..
        }
        | TableFactor::Table {
            alias: Some(TableAlias { name, .. }),
            ..
        }
        | TableFactor::Derived {
            alias: TableAlias { name, .. },
            ..
        }
        | TableFactor::Series {
            alias: TableAlias { name, .. },
            ..
        }
        | TableFactor::Dictionary {
            alias: TableAlias { name, .. },
            ..
        } => name,
    }
}
