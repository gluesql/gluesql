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
}
