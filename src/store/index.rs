use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum IndexError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("index name already exists: {0}")]
    IndexNameAlreadyExists(String),

    #[error("index name does not exist: {0}")]
    IndexNameDoesNotExist(String),

    #[error("conflict - update failed - index value")]
    ConflictOnEmptyIndexValueUpdate,

    #[error("conflict - delete failed - index value")]
    ConflictOnEmptyIndexValueDelete,

    #[error("conflict - scan failed - index value")]
    ConflictOnEmptyIndexValueScan,

    #[error("conflict - index sync - delete index data")]
    ConflictOnIndexDataDeleteSync,
}
