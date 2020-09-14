use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum EvaluateError {
    #[error("nested select row not found")]
    NestedSelectRowNotFound,

    #[error("literal add on non-numeric")]
    LiteralAddOnNonNumeric,

    #[error("unsupported compound identifier {0}")]
    UnsupportedCompoundIdentifier(String),

    #[error("unreachable condition base")]
    UnreachableConditionBase,

    #[error("unreachable evaluated arithmetic")]
    UnreachableEvaluatedArithmetic,

    #[error("unreachable literal arithmetic")]
    UnreachableLiteralArithmetic,

    #[error("unreachable, aggregated field not found {0}")]
    UnreachableAggregatedField(String),

    #[error("unreachable, aggregated field does not exist")]
    UnreachableEmptyAggregated,

    #[error("unimplemented")]
    Unimplemented,
}
