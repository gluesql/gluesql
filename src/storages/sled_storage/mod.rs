use sled::{self, Config, Db};
use std::convert::TryFrom;
use std::str;
use thiserror::Error as ThisError;

#[cfg(not(feature = "alter-table"))]
use crate::AlterTable;
#[cfg(feature = "alter-table")]
use crate::AlterTableError;
use crate::{Error, Result, Schema, StoreError};

#[cfg(feature = "alter-table")]
mod alter_table;
#[cfg(not(feature = "alter-table"))]
impl AlterTable for SledStorage {}

mod store;

#[derive(ThisError, Debug)]
enum StorageError {
    #[error(transparent)]
    Store(#[from] StoreError),

    #[cfg(feature = "alter-table")]
    #[error(transparent)]
    AlterTable(#[from] AlterTableError),

    #[error(transparent)]
    Sled(#[from] sled::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    Str(#[from] str::Utf8Error),
}

impl Into<Error> for StorageError {
    fn into(self) -> Error {
        use StorageError::*;

        match self {
            Sled(e) => Error::Storage(Box::new(e)),
            Bincode(e) => Error::Storage(e),
            Str(e) => Error::Storage(Box::new(e)),
            Store(e) => e.into(),

            #[cfg(feature = "alter-table")]
            AlterTable(e) => e.into(),
        }
    }
}

#[macro_export]
macro_rules! try_into {
    ($expr: expr) => {
        $expr.map_err(|e| {
            let e: StorageError = e.into();
            let e: Error = e.into();

            e
        })?
    };
    ($self: expr, $expr: expr) => {
        match $expr {
            Err(e) => {
                let e: StorageError = e.into();
                let e: Error = e.into();

                return Err(($self, e));
            }
            Ok(v) => v,
        }
    };
}

#[derive(Debug)]
pub struct SledStorage {
    tree: Db,
}

impl SledStorage {
    pub fn new(filename: &str) -> Result<Self> {
        let tree = try_into!(sled::open(filename));

        Ok(Self { tree })
    }
}

impl TryFrom<Config> for SledStorage {
    type Error = Error;

    fn try_from(config: Config) -> Result<Self> {
        let tree = try_into!(config.open());

        Ok(Self { tree })
    }
}

fn fetch_schema(tree: &Db, table_name: &str) -> Result<(String, Schema)> {
    let key = format!("schema/{}", table_name);
    let value = try_into!(tree.get(&key.as_bytes()));
    let value = value.ok_or(StoreError::SchemaNotFound)?;
    let schema = try_into!(bincode::deserialize(&value));

    Ok((key, schema))
}
