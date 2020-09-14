use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum AggregateError {
    #[error("unsupported compound identifier: {0}")]
    UnsupportedCompoundIdentifier(String),

    #[error("unsupported aggregation: {0}")]
    UnsupportedAggregation(String),

    #[error("only identifier is allowed in aggregation")]
    OnlyIdentifierAllowed,

    #[error("floating point numbers cannot be grouped")]
    FloatCannotBeGrouped,

    #[error("unreachable")]
    Unreachable,
}
