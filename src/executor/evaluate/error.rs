use {
    crate::ast::{Aggregate, Expr},
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum EvaluateError {
    #[error("nested select row not found")]
    NestedSelectRowNotFound,

    #[error("literal add on non-numeric")]
    LiteralAddOnNonNumeric,

    #[error("function requires string value: {0}")]
    FunctionRequiresStringValue(String),

    #[error("function requires integer value: {0}")]
    FunctionRequiresIntegerValue(String),

    #[error("function requires float or integer value: {0}")]
    FunctionRequiresFloatOrIntegerValue(String),

    #[error("function requires usize value: {0}")]
    FunctionRequiresUSizeValue(String),

    #[error("function requires float value: {0}")]
    FunctionRequiresFloatValue(String),

    #[error("extract format does not support value: {0}")]
    ExtractFormatNotMatched(String),

    #[error("value not found: {0}")]
    ValueNotFound(String),

    #[error("only boolean value is accepted: {0}")]
    BooleanTypeRequired(String),

    #[error("unsupported compound identifier {0:#?}")]
    UnsupportedCompoundIdentifier(Expr),

    #[error("unsupported stateless expression: {0:#?}")]
    UnsupportedStatelessExpr(Expr),

    #[error("unreachable wildcard expression")]
    UnreachableWildcardExpr,

    #[error("unreachable empty context")]
    UnreachableEmptyContext,

    #[error("unreachable empty aggregate value: {0:?}")]
    UnreachableEmptyAggregateValue(Aggregate),

    #[error("divisor should not be zero")]
    InvalidDivisorZero,
}
