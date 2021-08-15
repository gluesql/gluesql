use std::str;
use thiserror::Error as ThisError;

#[cfg(feature = "alter-table")]
use crate::AlterTableError;

#[cfg(feature = "index")]
use crate::IndexError;

use crate::Error;

#[derive(ThisError, Debug)]
pub enum StorageError {
    #[cfg(feature = "alter-table")]
    #[error(transparent)]
    AlterTable(#[from] AlterTableError),

    #[cfg(feature = "index")]
    #[error(transparent)]
    Index(#[from] IndexError),

    #[error(transparent)]
    Sled(#[from] sled::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    Str(#[from] str::Utf8Error),
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Error {
        use StorageError::*;

        match e {
            Sled(e) => Error::Storage(Box::new(e)),
            Bincode(e) => Error::Storage(e),
            Str(e) => Error::Storage(Box::new(e)),

            #[cfg(feature = "alter-table")]
            AlterTable(e) => e.into(),

            #[cfg(feature = "index")]
            Index(e) => e.into(),
        }
    }
}

pub fn err_into<E>(e: E) -> Error
where
    E: Into<StorageError>,
{
    let e: StorageError = e.into();
    let e: Error = e.into();

    e
}
