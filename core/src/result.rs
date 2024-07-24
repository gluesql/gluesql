use {serde::Serialize, std::fmt::Debug, thiserror::Error as ThisError};

use crate::data::schema::SchemaError;
pub use crate::{
    ast_builder::AstBuilderError,
    data::{
        ConvertError, IntervalError, KeyError, LiteralError, RowError, SchemaParseError,
        StringExtError, TableError, ValueError,
    },
    executor::{
        AggregateError, AlterError, DeleteError, EvaluateError, ExecuteError, FetchError,
        InsertError, SelectError, SortError, UpdateError, ValidateError,
    },
    plan::PlanError,
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

    #[error("ast-builder: {0}")]
    AstBuilder(#[from] AstBuilderError),

    #[error("alter-table: {0}")]
    AlterTable(#[from] AlterTableError),
    #[error("index: {0}")]
    Index(#[from] IndexError),
    #[error("execute: {0}")]
    Execute(#[from] ExecuteError),
    #[error("alter: {0}")]
    Alter(#[from] AlterError),
    #[error("fetch: {0}")]
    Fetch(#[from] FetchError),
    #[error("select: {0}")]
    Select(#[from] SelectError),
    #[error("evaluate: {0}")]
    Evaluate(#[from] EvaluateError),
    #[error("aggregate: {0}")]
    Aggregate(#[from] AggregateError),
    #[error("sort: {0}")]
    Sort(#[from] SortError),
    #[error("insert: {0}")]
    Insert(#[from] InsertError),
    #[error("update: {0}")]
    Delete(#[from] DeleteError),
    #[error("delete: {0}")]
    Update(#[from] UpdateError),
    #[error("table: {0}")]
    Table(#[from] TableError),
    #[error("validate: {0}")]
    Validate(#[from] ValidateError),
    #[error("row: {0}")]
    Row(#[from] RowError),
    #[error("key: {0}")]
    Key(#[from] KeyError),
    #[error("value: {0}")]
    Value(#[from] ValueError),
    #[error("convert: {0}")]
    Convert(#[from] ConvertError),
    #[error("literal: {0}")]
    Literal(#[from] LiteralError),
    #[error("interval: {0}")]
    Interval(#[from] IntervalError),
    #[error("string-ext: {0}")]
    StringExt(#[from] StringExtError),
    #[error("plan: {0}")]
    Plan(#[from] PlanError),
    #[error("schema-parse: {0}")]
    Schema(#[from] SchemaParseError),
    #[error("schema: {0}")]
    SchemaError(#[from] SchemaError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
