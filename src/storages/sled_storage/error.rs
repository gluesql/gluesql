use {crate::Error, sled::transaction::TransactionError, std::str, thiserror::Error as ThisError};

#[cfg(feature = "alter-table")]
use crate::AlterTableError;

#[derive(ThisError, Debug)]
pub enum StorageError {
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

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Error {
        use StorageError::*;

        match e {
            Sled(e) => Error::Storage(Box::new(e)),
            Bincode(e) => Error::Storage(e),
            Str(e) => Error::Storage(Box::new(e)),

            #[cfg(feature = "alter-table")]
            AlterTable(e) => e.into(),
        }
    }
}

impl Into<Error> for TransactionError<Error> {
    fn into(self) -> Error {
        match self {
            TransactionError::Abort(e) => e,
            TransactionError::Storage(e) => StorageError::Sled(e).into(),
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
