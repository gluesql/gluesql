use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum QueryBuilderError {
    #[error("failed to parse numeric value: {0}")]
    FailedToParseNumeric(String),

    #[error("hash join executor can only be built as an execution plan")]
    HashJoinExecutorRequiresPlan,

    #[error("index_by can only be built as an execution plan")]
    IndexByRequiresPlan,

    #[error(
        "projection expression label cannot be derived from a plan-only expression; use an explicit alias"
    )]
    ProjectionLabelRequiresAlias,

    #[error("values_from requires at least one row")]
    ValuesFromRequiresRows,

    #[error("values_from row has {found} values but {expected} columns are declared")]
    ValuesFromColumnCountMismatch { expected: usize, found: usize },
}
