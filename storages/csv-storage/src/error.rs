use {gluesql_core::error::Error, thiserror::Error};

pub trait ResultExt<T, E: ToString> {
    fn map_storage_err(self) -> Result<T, Error>;
}

impl<T, E: ToString> ResultExt<T, E> for std::result::Result<T, E> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

pub trait OptionExt<T, E: ToString> {
    fn map_storage_err(self, error: E) -> Result<T, Error>;
}

impl<T, E: ToString> OptionExt<T, E> for std::option::Option<T> {
    fn map_storage_err(self, error: E) -> Result<T, Error> {
        self.ok_or_else(|| error.to_string())
            .map_err(Error::StorageMsg)
    }
}

impl From<CsvStorageError> for Error {
    fn from(error: CsvStorageError) -> Self {
        Error::StorageMsg(error.to_string())
    }
}

#[derive(Error, Debug)]
pub enum CsvStorageError {
    #[error("file not found")]
    FileNotFound,

    #[error("table does not exist")]
    TableDoesNotExist,

    #[error("table name does not match with file")]
    TableNameDoesNotMatchWithFile,

    #[error("unreachable map type data row found")]
    UnreachableMapTypeDataRowFound,

    #[error("unreachable vector data row type found")]
    UnreachableVecTypeDataRowTypeFound,
}
