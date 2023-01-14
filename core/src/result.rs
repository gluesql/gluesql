use {
    crate::{
        ast_builder::AstBuilderError,
        data::{
            IntervalError, KeyError, LiteralError, RowError, StringExtError, TableError, ValueError,
        },
        executor::{
            AggregateError, AlterError, EvaluateError, ExecuteError, FetchError, InsertError,
            SelectError, SortError, UpdateError, ValidateError,
        },
        plan::PlanError,
        store::{GStore, GStoreMut},
        translate::TranslateError,
    },
    serde::Serialize,
    std::{error::Error as StdError, fmt::Debug, ops::ControlFlow},
    thiserror::Error as ThisError,
};

#[cfg(feature = "alter-table")]
use crate::store::AlterTableError;

#[cfg(feature = "index")]
use crate::store::IndexError;

#[derive(ThisError, Serialize, Debug)]
pub enum Error {
    #[error(transparent)]
    #[serde(with = "stringify")]
    Storage(#[from] Box<dyn StdError + Send + Sync + 'static>),

    #[error("storage error: {0}")]
    StorageMsg(String),

    #[error("parsing failed: {0}")]
    Parser(String),

    #[error(transparent)]
    Translate(#[from] TranslateError),

    #[error(transparent)]
    AstBuilder(#[from] AstBuilderError),

    #[cfg(feature = "alter-table")]
    #[error(transparent)]
    AlterTable(#[from] AlterTableError),

    #[cfg(feature = "index")]
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type MutResult<T, U> = std::result::Result<(T, U), (T, Error)>;

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        use Error::*;

        match (self, other) {
            (Parser(e), Parser(e2)) => e == e2,
            (StorageMsg(e), StorageMsg(e2)) => e == e2,
            (Translate(e), Translate(e2)) => e == e2,
            (AstBuilder(e), AstBuilder(e2)) => e == e2,
            #[cfg(feature = "alter-table")]
            (AlterTable(e), AlterTable(e2)) => e == e2,
            #[cfg(feature = "index")]
            (Index(e), Index(e2)) => e == e2,
            (Execute(e), Execute(e2)) => e == e2,
            (Alter(e), Alter(e2)) => e == e2,
            (Fetch(e), Fetch(e2)) => e == e2,
            (Select(e), Select(e2)) => e == e2,
            (Evaluate(e), Evaluate(e2)) => e == e2,
            (Aggregate(e), Aggregate(e2)) => e == e2,
            (Sort(e), Sort(e2)) => e == e2,
            (Insert(e), Insert(e2)) => e == e2,
            (Update(e), Update(e2)) => e == e2,
            (Table(e), Table(e2)) => e == e2,
            (Validate(e), Validate(e2)) => e == e2,
            (Row(e), Row(e2)) => e == e2,
            (Key(e), Key(e2)) => e == e2,
            (Value(e), Value(e2)) => e == e2,
            (Literal(e), Literal(e2)) => e == e2,
            (Interval(e), Interval(e2)) => e == e2,
            (StringExt(e), StringExt(e2)) => e == e2,
            (Plan(e), Plan(e2)) => e == e2,
            _ => false,
        }
    }
}

pub trait TrySelf<V>
where
    Self: Sized,
{
    fn try_self<T: GStore + GStoreMut>(self, storage: T) -> MutResult<T, V>;
}

impl<V> TrySelf<V> for Result<V> {
    fn try_self<T: GStore + GStoreMut>(self, storage: T) -> MutResult<T, V> {
        match self {
            Ok(v) => Ok((storage, v)),
            Err(e) => Err((storage, e)),
        }
    }
}

mod stringify {
    use {serde::Serializer, std::fmt::Display};

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Display,
        S: Serializer,
    {
        serializer.collect_str(value)
    }
}

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
