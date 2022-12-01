use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum AggregateError {
    #[error("unreachable rc unwrap failure")]
    UnreachableRcUnwrapFailure,
}
