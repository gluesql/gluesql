use {
    serde::Serialize,
    serde_json::Error as JSONError,
    std::{
        fmt,
        fmt::{Debug, Display},
    },
    thiserror::Error as TError,
};

#[derive(TError, Serialize, Debug, PartialEq)]
pub enum ParameterError {
    JSON(String),
    Notfound(String),
}

impl Display for ParameterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Self::JSON(v) => write!(f, "json: {}", &v),
            Self::Notfound(v) => write!(f, "parameter {} not found.", &v),
        }
    }
}

impl From<JSONError> for ParameterError {
    fn from(e: JSONError) -> Self {
        Self::JSON(format!("{}", &e))
    }
}
