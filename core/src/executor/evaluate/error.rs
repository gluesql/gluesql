use {
    crate::ast::{Aggregate, Expr},
    chrono::format::ParseErrorKind::*,
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum EvaluateError {
    #[error(transparent)]
    ChronoFormat(#[from] ChronoFormatError),

    #[error("nested select row not found")]
    NestedSelectRowNotFound,

    #[error("literal add on non-numeric")]
    LiteralAddOnNonNumeric,

    #[error("function requires string value: {0}")]
    FunctionRequiresStringValue(String),

    #[error("function requires integer value: {0}")]
    FunctionRequiresIntegerValue(String),

    #[error("function requires float or integer value: {0}")]
    FunctionRequiresFloatOrIntegerValue(String),

    #[error("function requires usize value: {0}")]
    FunctionRequiresUSizeValue(String),

    #[error("function requires float value: {0}")]
    FunctionRequiresFloatValue(String),

    #[error("extract format does not support value: {0}")]
    ExtractFormatNotMatched(String),

    #[error("function requires map value: {0}")]
    FunctionRequiresMapValue(String),

    #[error("value not found: {0}")]
    ValueNotFound(String),

    #[error("only boolean value is accepted: {0}")]
    BooleanTypeRequired(String),

    #[error("unsupported stateless expression: {0:#?}")]
    UnsupportedStatelessExpr(Expr),

    #[error("unreachable empty context")]
    UnreachableEmptyContext,

    #[error("unreachable empty aggregate value: {0:?}")]
    UnreachableEmptyAggregateValue(Aggregate),

    #[error("the divisor should not be zero")]
    DivisorShouldNotBeZero,

    #[error("negative substring length not allowed")]
    NegativeSubstrLenNotAllowed,

    #[error("subquery returns more than one row")]
    MoreThanOneRowReturned,

    #[error("format function does not support following data_type: {0}")]
    UnsupportedExprForFormatFunction(String),
}

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum ChronoFormatError {
    /// Given field is out of permitted range.
    #[error("given field is out of permitted range")]
    OutOfRange,

    /// There is no possible date and time value with given set of fields.
    ///
    /// This does not include the out-of-range conditions, which are trivially invalid.
    /// It includes the case that there are one or more fields that are inconsistent to each other.
    #[error("the given date and time value is impossible to be formmated")]
    Impossible,

    /// Given set of fields is not enough to make a requested date and time value.
    ///
    /// Note that there *may* be a case that given fields constrain the possible values so much
    /// that there is a unique possible value. Chrono only tries to be correct for
    /// most useful sets of fields however, as such constraint solving can be expensive.
    #[error("given set of field is not enough to be formatted")]
    NotEnough,

    /// The input string has some invalid character sequence for given formatting items.
    #[error("given format string has invalid specifier")]
    Invalid,

    /// The input string has been prematurely ended.
    #[error("input string has been permaturely ended")]
    TooShort,

    /// All formatting items have been read but there is a remaining input.
    #[error("given format string is missing some specifier")]
    TooLong,

    /// There was an error on the formatting string, or there were non-supported formating items.
    #[error("given format string includes non-supported formmating item")]
    BadFormat,

    // TODO: Change this- to `#[non_exhaustive]` (on the enum) when MSRV is increased
    #[error("unreachable chrono format error")]
    Unreachable,
}

impl ChronoFormatError {
    pub fn err_into(error: chrono::format::ParseError) -> crate::result::Error {
        let error: ChronoFormatError = error.into();
        let error: EvaluateError = error.into();
        error.into()
    }
}

impl From<chrono::format::ParseError> for ChronoFormatError {
    fn from(error: chrono::format::ParseError) -> ChronoFormatError {
        match error.kind() {
            OutOfRange => ChronoFormatError::OutOfRange,
            Impossible => ChronoFormatError::Impossible,
            NotEnough => ChronoFormatError::NotEnough,
            Invalid => ChronoFormatError::Invalid,
            TooShort => ChronoFormatError::TooShort,
            TooLong => ChronoFormatError::TooLong,
            BadFormat => ChronoFormatError::BadFormat,
            __Nonexhaustive => ChronoFormatError::Unreachable,
        }
    }
}
