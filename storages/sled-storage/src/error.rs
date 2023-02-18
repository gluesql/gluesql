use {
    gluesql_core::{
        result::Error,
        store::{AlterTableError, IndexError},
    },
    sled::transaction::TransactionError as SledTransactionError,
    std::{str, time},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Debug)]
pub enum StorageError {
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
    #[error(transparent)]
    TryFromSlice(#[from] std::array::TryFromSliceError),
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Error {
        use StorageError::*;

        match e {
            Sled(e) => Error::StorageMsg(e.to_string()),
            Bincode(e) => Error::StorageMsg(e.to_string()),
            Str(e) => Error::StorageMsg(e.to_string()),
            SystemTime(e) => Error::StorageMsg(e.to_string()),
            TryFromSlice(e) => Error::StorageMsg(e.to_string()),
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
