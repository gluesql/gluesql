use {gluesql_core::result::Error, thiserror::Error as ThisError};

#[derive(ThisError, Debug)]
pub enum StorageError {
    #[error("cannot import file as table: {0}")]
    InvalidFileImport(String),

    #[error(transparent)]
    Csv(#[from] csv::Error),
}

impl From<StorageError> for Error {
    fn from(storage_error: StorageError) -> Error {
        use StorageError::*;

        match storage_error {
            error @ InvalidFileImport(_) => Error::Storage(Box::new(error)),
            Csv(error) => Error::Storage(Box::new(error)),
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
