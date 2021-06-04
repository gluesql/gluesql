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

    #[error("function requires string value: {0}")]
    FunctionRequiresIntegerValue(String),

    #[error("function requires usize value: {0}")]
    FunctionRequiresUSizeValue(String),

    #[error(
        "number of function parameters not matching (expected: {expected:?}, found: {found:?})"
    )]
    NumberOfFunctionParamsNotMatching { expected: usize, found: usize },

    #[error("value not found: {0}")]
    ValueNotFound(String),

    #[error("only boolean value is accepted: {0}")]
    BooleanTypeRequired(String),

    #[error("unsupported compound identifier {0}")]
    UnsupportedCompoundIdentifier(String),

    #[error("unreachable wildcard expression")]
    UnreachableWildcardExpr,

    #[error("unsupported stateless expression: {0}")]
    UnsupportedStatelessExpr(String),

    #[error("unreachable empty context")]
    UnreachableEmptyContext,

    #[error("unreachable named function argument: {0}")]
    UnreachableFunctionArg(String),
}
