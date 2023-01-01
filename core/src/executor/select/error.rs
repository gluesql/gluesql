use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum SelectError {
    #[error("VALUES lists must all be the same length")]
    NumberOfValuesDifferent,
}
