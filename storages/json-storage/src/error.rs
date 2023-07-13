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

#[derive(Error, Debug)]
pub enum JsonStorageError {
    #[error("file not found")]
    FileNotFound,

    #[error("table does not exist")]
    TableDoesNotExist,

    #[error("column does not exist: {0}")]
    ColumnDoesNotExist(String),

    #[error("table name does not match with file")]
    TableNameDoesNotMatchWithFile,

    #[error("both {0}.jsonl and {0}.json files exist. remove or rename one")]
    BothJsonlAndJsonExist(String),

    #[error("invalid log file content: {0}")]
    InvalidJsonContent(String),

    #[error("json object type is required")]
    JsonObjectTypeRequired,

    #[error("json array type is required")]
    JsonArrayTypeRequired,
}
