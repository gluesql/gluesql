use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum FilterError {
    #[error("unimplemented")]
    Unimplemented,
}
