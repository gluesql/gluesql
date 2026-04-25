use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum AstBuilderError {
    #[error("failed to parse numeric value: {0}")]
    FailedToParseNumeric(String),

    #[error(
        "UNION branch must not carry ORDER BY, LIMIT, or OFFSET; use .alias_as() to nest it as a subquery first"
    )]
    UnionBranchWithClause,
}
