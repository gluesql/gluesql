use {
    crate::ast::DataType,
    bigdecimal::BigDecimal,
    serde::Serialize,
    std::{
        borrow::Cow,
        fmt::{self, Debug, Display},
    },
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum LiteralError {
    #[error("literal {literal} is incompatible with data type {data_type:?}")]
    IncompatibleLiteralForDataType {
        data_type: DataType,
        literal: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal<'a> {
    Number(Cow<'a, BigDecimal>),
    Text(Cow<'a, str>),
}

impl Display for Literal<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Number(n) => write!(f, "{n}"),
            Literal::Text(t) => f.write_str(t),
        }
    }
}
