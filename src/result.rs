use {
    crate::{
        data::{IntervalError, LiteralError, RowError, TableError, ValueError},
        executor::{
            AggregateError, AlterError, BlendError, EvaluateError, ExecuteError, FetchError,
            SelectError, UpdateError, ValidateError,
        },
        translate::TranslateError,
    },
    serde::Serialize,
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
    Storage(#[from] Box<dyn std::error::Error>),

    #[error("parsing failed: {0}")]
    Parser(String),

    #[error(transparent)]
    Translate(#[from] TranslateError),

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
    Evaluate(#[from] EvaluateError),
    #[error(transparent)]
    Select(#[from] SelectError),
    #[error(transparent)]
    Blend(#[from] BlendError),
    #[error(transparent)]
    Aggregate(#[from] AggregateError),
    #[error(transparent)]
    Update(#[from] UpdateError),
    #[error(transparent)]
    Row(#[from] RowError),
    #[error(transparent)]
    Table(#[from] TableError),
    #[error(transparent)]
    Validate(#[from] ValidateError),
    #[error(transparent)]
    Value(#[from] ValueError),
    #[error(transparent)]
    Literal(#[from] LiteralError),
    #[error(transparent)]
    Interval(#[from] IntervalError),
}

pub type Result<T> = std::result::Result<T, Error>;
pub type MutResult<T, U> = std::result::Result<(T, U), (T, Error)>;

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        use Error::*;

        match (self, other) {
            (Parser(e), Parser(e2)) => e == e2,
            (Translate(e), Translate(e2)) => e == e2,
            #[cfg(feature = "alter-table")]
            (AlterTable(e), AlterTable(e2)) => e == e2,
            #[cfg(feature = "index")]
            (Index(e), Index(e2)) => e == e2,
            (Execute(e), Execute(e2)) => e == e2,
            (Alter(e), Alter(e2)) => e == e2,
            (Fetch(e), Fetch(e2)) => e == e2,
            (Evaluate(e), Evaluate(e2)) => e == e2,
            (Select(e), Select(e2)) => e == e2,
            (Blend(e), Blend(e2)) => e == e2,
            (Aggregate(e), Aggregate(e2)) => e == e2,
            (Update(e), Update(e2)) => e == e2,
            (Row(e), Row(e2)) => e == e2,
            (Table(e), Table(e2)) => e == e2,
            (Validate(e), Validate(e2)) => e == e2,
            (Value(e), Value(e2)) => e == e2,
            (Literal(e), Literal(e2)) => e == e2,
            (Interval(e), Interval(e2)) => e == e2,
            _ => false,
        }
    }
}

mod stringify {
    use serde::Serializer;
    use std::fmt::Display;

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Display,
        S: Serializer,
    {
        serializer.collect_str(value)
    }
}
