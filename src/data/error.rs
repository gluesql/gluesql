use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum DataError {
    #[error("impossible cast")]
    ImpossibleCast,

    #[error("unreachable impossible cast")]
    UnreachableImpossibleCast,

    #[error("unimplemented cast")]
    UnimplementedCast,
}
