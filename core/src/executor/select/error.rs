use {serde::Serialize, std::fmt::Debug, thiserror::Error as ThisError};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum SelectError {
    #[error("table alias not found: {0}")]
    TableAliasNotFound(String),

    #[error("table alias for blend not found: {0}")]
    BlendTableAliasNotFound(String),

    #[cfg(not(feature = "sorter"))]
    #[error("order by on non-indexed expression is not supported: {0:?}")]
    OrderByOnNonIndexedExprNotSupported(Vec<crate::ast::OrderByExpr>),

    #[error("unreachable!")]
    Unreachable,
}
