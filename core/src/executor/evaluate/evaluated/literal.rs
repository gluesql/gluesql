mod convert;
mod error;

pub(crate) use convert::{literal_to_value, try_cast_literal_to_value};
pub use error::LiteralError;

use {
    bigdecimal::BigDecimal,
    std::{
        borrow::Cow,
        fmt::{self, Display},
    },
};

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
