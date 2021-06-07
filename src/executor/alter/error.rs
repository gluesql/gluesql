use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum AlterError {
    // CREATE TABLE
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    // CREATE INDEX, DROP TABLE
    #[error("table does not exist: {0}")]
    TableNotFound(String),

    // validate column def
    #[error("column '{0}' of data type '{1}' is unsupported for unique constraint")]
    UnsupportedDataTypeForUniqueColumn(String, String),

    // validate index expr
    #[error("unsupported index expr: {0}")]
    UnsupportedIndexExpr(String),

    #[error("identifier not found: {0}")]
    IdentifierNotFound(String),
}
