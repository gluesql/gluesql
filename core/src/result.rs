use {serde::Serialize, std::fmt::Debug, thiserror::Error as ThisError};

pub use crate::{
    data::{IntervalError, KeyError, SchemaParseError, StringExtError, TableError, ValueError},
    executor::{
        AlterError, DeleteError, EvaluateError, ExecuteError, FetchError, InsertError, QueryError,
        UpdateError, ValidateError,
    },
    planner::PlannerError,
    query_builder::QueryBuilderError,
    row_conversion::RowConversionError,
    store::{AlterTableError, IndexError},
    translate::TranslateError,
};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum Error {
    #[error("storage: {0}")]
    StorageMsg(String),

    #[error("parser: {0}")]
    Parser(String),

    #[error("translate: {0}")]
    Translate(#[from] TranslateError),

    #[error("query-builder: {0}")]
    QueryBuilder(#[from] QueryBuilderError),

    #[error("alter-table: {0}")]
    AlterTable(#[from] AlterTableError),
    #[error("index: {0}")]
    Index(#[from] IndexError),
    #[error("execute: {0}")]
    Execute(#[from] ExecuteError),
    #[error("alter: {0}")]
    Alter(Box<AlterError>),
    #[error("fetch: {0}")]
    Fetch(#[from] FetchError),
    #[error("query: {0}")]
    Query(#[from] QueryError),
    #[error("evaluate: {0}")]
    Evaluate(#[from] EvaluateError),
    #[error("insert: {0}")]
    Insert(#[from] InsertError),
    #[error("delete: {0}")]
    Delete(#[from] DeleteError),
    #[error("update: {0}")]
    Update(#[from] UpdateError),
    #[error("table: {0}")]
    Table(#[from] TableError),
    #[error("validate: {0}")]
    Validate(#[from] ValidateError),
    #[error("key: {0}")]
    Key(#[from] KeyError),
    #[error("value: {0}")]
    Value(Box<ValueError>),
    #[error("interval: {0}")]
    Interval(#[from] IntervalError),
    #[error("string-ext: {0}")]
    StringExt(#[from] StringExtError),
    #[error("planner: {0}")]
    Planner(#[from] PlannerError),
    #[error("schema-parse: {0}")]
    Schema(#[from] SchemaParseError),

    #[error("row-conversion: {0}")]
    RowConversion(#[from] RowConversionError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<AlterError> for Error {
    fn from(e: AlterError) -> Error {
        Error::Alter(Box::new(e))
    }
}

impl From<ValueError> for Error {
    fn from(e: ValueError) -> Error {
        Error::Value(Box::new(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_error_prefixes() {
        let actual = Error::from(QueryError::ValuesLengthMismatch).to_string();
        let expected = "query: VALUES lists must all be the same length";
        assert_eq!(actual, expected);

        let actual = Error::from(DeleteError::ValueNotFound("id".to_owned())).to_string();
        let expected = "delete: Value not found on column: id";
        assert_eq!(actual, expected);

        let actual = Error::from(UpdateError::ColumnNotFound("id".to_owned())).to_string();
        let expected = "update: column not found id";
        assert_eq!(actual, expected);
    }
}
