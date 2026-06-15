use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum SelectError {
    #[error("VALUES list must have at least one row")]
    ValuesListEmpty,

    #[error("VALUES lists must all be the same length")]
    NumberOfValuesDifferent,

    #[error(
        "UNION column count mismatch: left returns {left} column(s) but right returns {right} column(s)"
    )]
    UnionColumnCountMismatch { left: usize, right: usize },

    /// Raised when `SELECT DISTINCT` is combined with an `ORDER BY` expression
    /// that does not appear in the select list.
    ///
    /// SQL standard (mirrored by PostgreSQL): DISTINCT eliminates non-projected
    /// columns before ORDER BY is evaluated, so ORDER BY can only reference
    /// output columns.
    #[error("for SELECT DISTINCT, ORDER BY expression '{0}' must appear in the select list")]
    DistinctOrderByNotInSelectList(String),
}
