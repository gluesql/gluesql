use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum EvaluateError {
    #[error("nested select row not found")]
    NestedSelectRowNotFound,

    #[error("literal add on non-numeric")]
    LiteralAddOnNonNumeric,

    #[error("function is not supported: {0}")]
    FunctionNotSupported(String),

    #[error("function requires string value: {0}")]
    FunctionRequiresStringValue(String),

    #[error("unsupported compound identifier {0}")]
    UnsupportedCompoundIdentifier(String),

    #[error("unreachable condition base")]
    UnreachableConditionBase,

    #[error("unreachable evaluated arithmetic")]
    UnreachableEvaluatedArithmetic,

    #[error("unreachable literal arithmetic")]
    UnreachableLiteralArithmetic,

    #[error("unreachable empty function argument")]
    UnreachableEmptyFunctionArg,

    #[error("unimplemented")]
    Unimplemented,
}
