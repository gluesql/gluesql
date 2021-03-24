use {
    crate::{
        data::{LiteralError, RowError, TableError, ValueError},
        executor::{
            AggregateError, BlendError, CreateTableError, EvaluateError, ExecuteError, FetchError,
            FilterError, JoinError, LimitError, SelectError, UpdateError, ValidateError,
        },
    },
    serde::Serialize,
    thiserror::Error as ThisError,
};

#[cfg(feature = "alter-table")]
use crate::store::AlterTableError;

#[derive(ThisError, Serialize, Debug)]
pub enum Error {
    #[cfg(feature = "alter-table")]
    #[error(transparent)]
    AlterTable(#[from] AlterTableError),

    #[error(transparent)]
    #[serde(with = "stringify")]
    Storage(#[from] Box<dyn std::error::Error>),

    #[error(transparent)]
    Execute(#[from] ExecuteError),
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error(transparent)]
    Evaluate(#[from] EvaluateError),
    #[error(transparent)]
    Select(#[from] SelectError),
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error(transparent)]
    Blend(#[from] BlendError),
    #[error(transparent)]
    Aggregate(#[from] AggregateError),
    #[error(transparent)]
    Update(#[from] UpdateError),
    #[error(transparent)]
    Filter(#[from] FilterError),
    #[error(transparent)]
    Limit(#[from] LimitError),
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
    CreateTable(#[from] CreateTableError),
}

pub type Result<T> = std::result::Result<T, Error>;
pub type MutResult<T, U> = std::result::Result<(T, U), (T, Error)>;

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        use Error::*;

        match (self, other) {
            #[cfg(feature = "alter-table")]
            (AlterTable(e), AlterTable(e2)) => e == e2,
            (Execute(e), Execute(e2)) => e == e2,
            (Fetch(e), Fetch(e2)) => e == e2,
            (Evaluate(e), Evaluate(e2)) => e == e2,
            (Select(e), Select(e2)) => e == e2,
            (Join(e), Join(e2)) => e == e2,
            (Blend(e), Blend(e2)) => e == e2,
            (Aggregate(e), Aggregate(e2)) => e == e2,
            (Update(e), Update(e2)) => e == e2,
            (Filter(e), Filter(e2)) => e == e2,
            (Limit(e), Limit(e2)) => e == e2,
            (Row(e), Row(e2)) => e == e2,
            (Table(e), Table(e2)) => e == e2,
            (Validate(e), Validate(e2)) => e == e2,
            (Value(e), Value(e2)) => e == e2,
            (Literal(e), Literal(e2)) => e == e2,
            (CreateTable(e), CreateTable(e2)) => e == e2,
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
