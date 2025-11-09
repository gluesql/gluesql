use {
    crate::{ast::DataType, data::StringExt},
    Literal::*,
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
    #[error("operator doesn't exist: {base} {case} {pattern}", case = if *case_sensitive { "LIKE" } else { "ILIKE" })]
    LikeOnNonString {
        base: String,
        pattern: String,
        case_sensitive: bool,
    },

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

impl<'a> Literal<'a> {
    #[must_use]
    pub fn concat(self, other: Literal<'_>) -> Self {
        let convert = |literal| match literal {
            Number(v) => v.to_string(),
            Text(v) => v.into_owned(),
        };

        Literal::Text(Cow::Owned(convert(self) + &convert(other)))
    }

    pub fn like(&self, other: &Literal<'a>, case_sensitive: bool) -> crate::result::Result<bool> {
        match (self, other) {
            (Text(l), Text(r)) => l.like(r, case_sensitive),
            _ => Err(LiteralError::LikeOnNonString {
                base: self.to_string(),
                pattern: other.to_string(),
                case_sensitive,
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Literal::{self, *},
        bigdecimal::BigDecimal,
        std::{borrow::Cow, str::FromStr},
    };

    fn text(value: &str) -> Literal<'static> {
        Text(Cow::Owned(value.to_owned()))
    }

    #[test]
    fn concat_literals() {
        macro_rules! num_literal {
            ($num: expr) => {
                Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }

        assert_eq!(text("Foo").concat(text("Bar")), text("FooBar"));
        assert_eq!(num_literal!("1").concat(num_literal!("2")), text("12"));
    }
}
