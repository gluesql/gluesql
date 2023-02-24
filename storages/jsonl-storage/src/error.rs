use {gluesql_core::result::Error, std::fmt};

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

pub enum JsonlStorageError {
    FileNotFound,
    TableDoesNotExist,
    ColumnDoesNotExist(String),
}

impl fmt::Display for JsonlStorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let payload = match self {
            JsonlStorageError::FileNotFound => "file not found".to_owned(),
            JsonlStorageError::TableDoesNotExist => "table does not exist".to_owned(),
            JsonlStorageError::ColumnDoesNotExist(column) => {
                format! {"column does not exist: {column}"}
            }
        };

        write!(f, "{}", payload)
    }
}
