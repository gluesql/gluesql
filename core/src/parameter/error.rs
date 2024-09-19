use {
    serde::Serialize,
    std::{
        fmt,
        fmt::{Debug, Display},
    },
    thiserror::Error as TError,
};

#[derive(TError, Serialize, Debug, PartialEq)]
pub enum ParameterError {
    Decode(String),
    Encode(String),
}

impl Display for ParameterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Self::Encode(v) => write!(f, "{}", &v),
            Self::Decode(v) => write!(f, "{}", &v),
        }
    }
}
