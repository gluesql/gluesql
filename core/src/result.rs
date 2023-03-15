use {
    crate::{
        ast_builder::AstBuilderError,
        data::{
            IntervalError, KeyError, LiteralError, RowError, SchemaParseError, StringExtError,
            TableError, ValueError,
        },
        executor::{
            AggregateError, AlterError, EvaluateError, ExecuteError, FetchError, InsertError,
            SelectError, SortError, UpdateError, ValidateError,
        },
        plan::PlanError,
        store::{AlterTableError, IndexError},
        translate::TranslateError,
    },
    serde::Serialize,
    std::{fmt::Debug, ops::ControlFlow},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum Error {
    #[error("storage error: {0}")]
    StorageMsg(String),

    #[error("parsing failed: {0}")]
    Parser(String),

    #[error(transparent)]
    Translate(#[from] TranslateError),

    #[error(transparent)]
    AstBuilder(#[from] AstBuilderError),

    #[error(transparent)]
    AlterTable(#[from] AlterTableError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error(transparent)]
    Execute(#[from] ExecuteError),
    #[error(transparent)]
    Alter(#[from] AlterError),
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error(transparent)]
    Select(#[from] SelectError),
    #[error(transparent)]
    Evaluate(#[from] EvaluateError),
    #[error(transparent)]
    Aggregate(#[from] AggregateError),
    #[error(transparent)]
    Sort(#[from] SortError),
    #[error(transparent)]
    Insert(#[from] InsertError),
    #[error(transparent)]
    Update(#[from] UpdateError),
    #[error(transparent)]
    Table(#[from] TableError),
    #[error(transparent)]
    Validate(#[from] ValidateError),
    #[error(transparent)]
    Row(#[from] RowError),
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(transparent)]
    Value(#[from] ValueError),
    #[error(transparent)]
    Literal(#[from] LiteralError),
    #[error(transparent)]
    Interval(#[from] IntervalError),
    #[error(transparent)]
    StringExt(#[from] StringExtError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Schema(#[from] SchemaParseError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait IntoControlFlow<T> {
    fn into_control_flow(self) -> ControlFlow<Result<T>, T>;
}

impl<T> IntoControlFlow<T> for Result<T> {
    fn into_control_flow(self) -> ControlFlow<Result<T>, T> {
        match self {
            Ok(v) => ControlFlow::Continue(v),
            e => ControlFlow::Break(e),
        }
    }
}
