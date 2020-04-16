use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum FilterError {
    #[error("nested select row not found")]
    NestedSelectRowNotFound,

    #[error("UnreachableConditionBase")]
    UnreachableConditionBase,

    #[error("unimplemented")]
    Unimplemented,
}
