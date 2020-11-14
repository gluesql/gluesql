use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum EvaluateError {
    #[error("nested select row not found")]
    NestedSelectRowNotFound,

    #[error("literal add on non-numeric")]
    LiteralAddOnNonNumeric,

    #[error("unary plus operation on non-numeric")]
    LiteralUnaryPlusOnNonNumeric,

    #[error("unary minus operation on non-numeric")]
    LiteralUnaryMinusOnNonNumeric,

    #[error("function is not supported: {0}")]
    FunctionNotSupported(String),

    #[error("function requires string value: {0}")]
    FunctionRequiresStringValue(String),

    #[error(
        "number of function parameters not matching (expected: {expected:?}, found: {found:?})"
    )]
    NumberOfFunctionParamsNotMatching { expected: usize, found: usize },

    #[error("value not found: {0}")]
    ValueNotFound(String),

    #[error("unsupported compound identifier {0}")]
    UnsupportedCompoundIdentifier(String),

    #[error("unreachable condition base")]
    UnreachableConditionBase,

    #[error("unreachable evaluated arithmetic")]
    UnreachableEvaluatedArithmetic,

    #[error("unreachable literal arithmetic")]
    UnreachableLiteralArithmetic,

    #[error("unimplemented")]
    Unimplemented,
}
