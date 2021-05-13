use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum AlterError {
    // CREATE TABLE
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    // DROP
    #[error("table does not exist: {0}")]
    TableNotFound(String),

    // validate column def
    #[error("column '{0}' of data type '{1}' is unsupported for unique constraint")]
    UnsupportedDataTypeForUniqueColumn(String, String),
}
