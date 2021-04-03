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

impl From<TransactionError<Error>> for Error {
    fn from(error: TransactionError<Error>) -> Error {
        match error {
            TransactionError::Abort(error) => error,
            TransactionError::Storage(error) => StorageError::Sled(error).into(),
        }
    }
}

pub fn err_into<E>(error: E) -> Error
where
    E: Into<StorageError>,
{
    let error: StorageError = error.into();
    let error: Error = error.into();

    error
}
