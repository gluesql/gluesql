use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum QueryError {
    #[error("VALUES lists must all be the same length")]
    ValuesLengthMismatch,

    #[error("ORDER BY column index must be within the SELECT list: {0}")]
    OrderByColumnIndexOutOfRange(usize),

    #[error("ORDER BY column index is too large: {0}")]
    OrderByColumnIndexTooLarge(String),

    #[error("table alias not found: {0}")]
    TableAliasNotFound(String),

    #[error("SERIES has invalid size: {0}")]
    InvalidSeriesSize(i64),

    #[error("table '{0}' has {1} columns available but {2} column aliases specified")]
    TooManyColumnAliases(String, usize, usize),

    #[error("table has no primary key column: {0}")]
    PrimaryKeyColumnNotFound(String),

    #[error("unreachable - table schema missing after source preparation: {0}")]
    UnreachableTableSchemaMissingAfterPreparation(String),

    #[error("unreachable - RIGHT JOIN reached execution without being lowered by the planner")]
    UnreachableUnplannedRightOuterJoin,

    #[error("unreachable - NULL-extended relations do not match the join sources: {0}")]
    UnreachableNullExtendRelationMismatch(String),
}
