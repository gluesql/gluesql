use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum ConvertError {
    #[error("unimplemented where expression: {0}")]
    Unimplemented(String),
    #[error("unimplemented in list in where expression")]
    UnimplementedInList,
    #[error("unsupported where expression")]
    Unsupported,
}
