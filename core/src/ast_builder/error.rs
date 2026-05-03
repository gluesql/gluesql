use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum AstBuilderError {
    #[error("failed to parse numeric value: {0}")]
    FailedToParseNumeric(String),

    #[error("hash join executor can only be built as an execution plan")]
    HashJoinExecutorRequiresPlan,

    #[error(
        "projection expression label cannot be derived from a plan-only expression; use an explicit alias"
    )]
    ProjectionLabelRequiresAlias,
}
