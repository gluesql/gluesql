use sled::{self, Config, Db};
use std::convert::TryFrom;
use std::str;
use thiserror::Error as ThisError;

#[cfg(not(feature = "alter-table"))]
use crate::AlterTable;
#[cfg(feature = "alter-table")]
use crate::AlterTableError;
use crate::{Error, Result, Schema};

#[cfg(feature = "alter-table")]
mod alter_table;
#[cfg(not(feature = "alter-table"))]
impl AlterTable for SledStorage {}

mod store;

#[derive(ThisError, Debug)]
enum StorageError {
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

            #[cfg(feature = "alter-table")]
            AlterTable(e) => e.into(),
        }
    }
}

fn err_into<E>(e: E) -> Error
where
    E: Into<StorageError>,
{
    let e: StorageError = e.into();
    let e: Error = e.into();

    e
}

#[derive(Debug, Clone)]
pub struct SledStorage {
    tree: Db,
}

impl SledStorage {
    pub fn new(filename: &str) -> Result<Self> {
        let tree = sled::open(filename).map_err(err_into)?;

        Ok(Self { tree })
    }
}

impl TryFrom<Config> for SledStorage {
    type Error = Error;

    fn try_from(config: Config) -> Result<Self> {
        let tree = config.open().map_err(err_into)?;

        Ok(Self { tree })
    }
}

fn fetch_schema(tree: &Db, table_name: &str) -> Result<(String, Option<Schema>)> {
    let key = format!("schema/{}", table_name);
    let value = tree.get(&key.as_bytes()).map_err(err_into)?;
    let schema = value
        .map(|v| bincode::deserialize(&v))
        .transpose()
        .map_err(err_into)?;

    Ok((key, schema))
}
