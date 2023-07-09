use {
    crate::ast::{DataType, Expr},
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum AlterError {
    // CREATE TABLE
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("function already exists: {0}")]
    FunctionAlreadyExists(String),

    #[error("function does not exist: {0}")]
    FunctionNotFound(String),

    // CREATE INDEX, DROP TABLE
    #[error("table does not exist: {0}")]
    TableNotFound(String),

    #[error("CTAS source table does not exist: {0}")]
    CtasSourceTableNotFound(String),

    // validate column def
    #[error("column '{0}' of data type '{1:?}' is unsupported for unique constraint")]
    UnsupportedDataTypeForUniqueColumn(String, DataType),

    // validate index expr
    #[error("unsupported index expr: {0:#?}")]
    UnsupportedIndexExpr(Expr),

    // validate index expr
    #[error("unsupported unnamed argument")]
    UnsupportedUnnamedArg,

    #[error("identifier not found: {0:#?}")]
    IdentifierNotFound(Expr),

    #[error("duplicate column name: {0}")]
    DuplicateColumnName(String),

    #[error("duplicate arg name: {0}")]
    DuplicateArgName(String),

    #[error("non-default argument should not follow the default argument")]
    NonDefaultArgumentFollowsDefaultArgument,
}
