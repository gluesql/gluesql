use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum FilterError {
    #[error("nested select row not found")]
    NestedSelectRowNotFound,

    #[error("literal add on non-numeric")]
    LiteralAddOnNonNumeric,

    #[error("unsupported compound identifier {0}")]
    UnsupportedCompoundIdentifier(String),

    #[error("unreachable condition base")]
    UnreachableConditionBase,

    #[error("unreachable parsed arithmetic")]
    UnreachableParsedArithmetic,

    #[error("unreachable literal arithmetic")]
    UnreachableLiteralArithmetic,

    #[error("unimplemented")]
    Unimplemented,
}
