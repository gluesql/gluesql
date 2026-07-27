use {serde::Serialize, std::fmt::Debug, thiserror::Error as ThisError};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum PlanError {
    /// Error that that omits when user projects common column name from multiple tables in `JOIN`
    /// situation.
    #[error("column reference {0} is ambiguous, please specify the table name")]
    ColumnReferenceAmbiguous(String),

    #[error(
        "SELECT * is not allowed for joins mixing schemaful and schemaless tables; use qualified wildcard or explicit columns"
    )]
    SchemalessMixedJoinWildcardProjection,

    #[error("specifying columns is not allowed for schemaless table INSERT; omit the column list")]
    SchemalessInsertWithExplicitColumns,

    #[error("unreachable")]
    Unreachable,

    #[error("window functions are only allowed in the SELECT projection: found in {0}")]
    WindowFunctionNotAllowedInClause(&'static str),

    #[error("window functions cannot be combined with GROUP BY, HAVING, or DISTINCT")]
    WindowWithGroupByHavingOrDistinctNotSupported,

    #[error("window functions cannot be nested inside an aggregate function")]
    WindowNestedInAggregate,

    #[error("window functions cannot be nested inside another window function")]
    WindowNestedInWindow,
}
