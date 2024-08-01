use {gluesql_core::prelude::Error, thiserror::Error};

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

impl From<ParquetStorageError> for Error {
    fn from(error: ParquetStorageError) -> Self {
        Self::StorageMsg(error.to_string())
    }
}

#[derive(Error, Debug)]
pub enum ParquetStorageError {
    #[error("file not found")]
    FileNotFound,

    #[error("table {0} does not exist")]
    TableDoesNotExist(String),

    #[error("Unexpected key type for map: received {0}, expected String")]
    UnexpectedKeyTypeForMap(String),

    #[error("Unreachable gluesql value type for parquet writer")]
    UnreachableGlueSqlValueTypeForParquetWriter,
}
