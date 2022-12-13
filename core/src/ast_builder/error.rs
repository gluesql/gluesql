use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum AstBuilderError {
    #[error("failed to parse numeric value: {0}")]
    FailedToParseNumeric(String),
}
