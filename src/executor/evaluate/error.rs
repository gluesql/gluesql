use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum EvaluateError {
    #[error("nested select row not found")]
    NestedSelectRowNotFound,

    #[error("literal add on non-numeric")]
    LiteralAddOnNonNumeric,

    #[error("unary operation on non-numeric")]
    LiteralUnaryOperationOnNonNumeric,

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

    #[error("unsupported compound identifier {0}")]
    UnsupportedCompoundIdentifier(String),

    #[error("unsupported literal binary arithmetic between {0} and {1}")]
    UnsupportedLiteralBinaryArithmetic(String, String),

    #[error("unsupported evaluated binary arithmetic between {0} and {1}")]
    UnsupportedEvaluatedBinaryArithmetic(String, String),

    #[error("unsupported evaluated unary arithmetic of {0}")]
    UnsupportedEvaluatedUnaryArithmetic(String),

    #[error("unimplemented")]
    Unimplemented,

    #[error("unreachable literal arithmetic")]
    UnreachableLiteralArithmetic,

    #[error("unreachable empty context")]
    UnreachableEmptyContext,

    #[error("unreachable named function argument: {0}")]
    UnreachableFunctionArg(String),
}
