use {crate::ast::Expr, serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum AggregateError {
    #[error("unsupported compound identifier: {0:#?}")]
    UnsupportedCompoundIdentifier(Expr),

    #[error("unsupported aggregation: {0}")]
    UnsupportedAggregation(String),

    #[error("only identifier is allowed in aggregation")]
    OnlyIdentifierAllowed,

    #[error("value not found: {0}")]
    ValueNotFound(String),

    #[error("unreachable")]
    Unreachable,

    #[error("unreachable named function arg: {0}")]
    UnreachableNamedFunctionArg(String),
}
