use {crate::ast::Expr, serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum AggregateError {
    #[error("unsupported compound identifier: {0:#?}")]
    UnsupportedCompoundIdentifier(Expr),

    #[error("only identifier is allowed in aggregation")]
    OnlyIdentifierAllowed,

    #[error("value not found: {0}")]
    ValueNotFound(String),

    #[error("unreachable rc unwrap failure")]
    UnreachableRcUnwrapFailure,

    #[error("unsupported aggregate function")]
    UnsupportedAggregateFunction,

    #[error("unsupported Aggregate literal")]
    UnsupportedAggregateLiteral,
}
