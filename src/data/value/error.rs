use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum ValueError {
    #[error("sql type not supported yet")]
    SqlTypeNotSupported,

    #[error("literal not supported yet")]
    LiteralNotSupported,

    #[error("ast expr not supported: {0}")]
    ExprNotSupported(String),

    #[error("failed to parse number")]
    FailedToParseNumber,

    #[error("add on non numeric value")]
    AddOnNonNumeric,

    #[error("subtract on non numeric value")]
    SubtractOnNonNumeric,

    #[error("multiply on non numeric value")]
    MultiplyOnNonNumeric,

    #[error("divide on non numeric value")]
    DivideOnNonNumeric,

    #[error("null value on not null field")]
    NullValueOnNotNullField,

    #[error("floating numbers cannot be grouped by")]
    FloatCannotBeGroupedBy,

    #[error("unary plus operation for non numeric value")]
    UnaryPlusOnNonNumeric,

    #[error("unary minus operation for non numeric value")]
    UnaryMinusOnNonNumeric,

    #[error("floating columns cannot be set to unique constraint")]
    ConflictOnFloatWithUniqueConstraint,

    #[error("impossible cast")]
    ImpossibleCast,

    #[error("unimplemented cast")]
    UnimplementedCast,
}
