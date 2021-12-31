use {
    gluesql_core::{result::Error, store::IndexError},
    sled::transaction::TransactionError as SledTransactionError,
    std::{str, time},
    thiserror::Error as ThisError,
};

#[cfg(feature = "alter-table")]
use gluesql_core::store::AlterTableError;

#[derive(ThisError, Debug)]
pub enum StorageError {
    #[cfg(feature = "alter-table")]
    #[error(transparent)]
    AlterTable(#[from] AlterTableError),
    #[error(transparent)]
    Index(#[from] IndexError),

    #[error(transparent)]
    Sled(#[from] sled::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    Str(#[from] str::Utf8Error),
    #[error(transparent)]
    SystemTime(#[from] time::SystemTimeError),
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Error {
        use StorageError::*;

        match e {
            Sled(e) => Error::Storage(Box::new(e)),
            Bincode(e) => Error::Storage(e),
            Str(e) => Error::Storage(Box::new(e)),
            SystemTime(e) => Error::Storage(Box::new(e)),

            #[cfg(feature = "alter-table")]
            AlterTable(e) => e.into(),
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

pub fn tx_err_into(e: SledTransactionError<Error>) -> Error {
    match e {
        SledTransactionError::Abort(e) => e,
        SledTransactionError::Storage(e) => StorageError::Sled(e).into(),
    }
}
