use thiserror::Error as ThisError;

use crate::data::{RowError, TableError, ValueError};
use crate::executor::{
    BlendError, FilterContextError, FilterError, JoinError, SelectError, UpdateError,
};
use crate::storage::StoreError;
use crate::ExecuteError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Storage(#[from] Box<dyn std::error::Error>),

    #[error(transparent)]
    Execute(#[from] ExecuteError),
    #[error(transparent)]
    Select(#[from] SelectError),
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error(transparent)]
    Blend(#[from] BlendError),
    #[error(transparent)]
    Update(#[from] UpdateError),
    #[error(transparent)]
    Filter(#[from] FilterError),
    #[error(transparent)]
    FilterContext(#[from] FilterContextError),
    #[error(transparent)]
    Row(#[from] RowError),
    #[error(transparent)]
    Table(#[from] TableError),
    #[error(transparent)]
    Value(#[from] ValueError),
}

pub type Result<T> = std::result::Result<T, Error>;

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        use Error::*;

        match (self, other) {
            (Store(e), Store(e2)) => e == e2,
            (Execute(e), Execute(e2)) => e == e2,
            (Select(e), Select(e2)) => e == e2,
            (Join(e), Join(e2)) => e == e2,
            (Blend(e), Blend(e2)) => e == e2,
            (Update(e), Update(e2)) => e == e2,
            (Filter(e), Filter(e2)) => e == e2,
            (FilterContext(e), FilterContext(e2)) => e == e2,
            (Row(e), Row(e2)) => e == e2,
            (Table(e), Table(e2)) => e == e2,
            (Value(e), Value(e2)) => e == e2,
            _ => false,
        }
    }
}
