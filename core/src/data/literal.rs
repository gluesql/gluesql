use {
    super::StringExt,
    crate::{
        ast::AstLiteral,
        result::{Error, Result},
    },
    bigdecimal::BigDecimal,
    serde::Serialize,
    std::{borrow::Cow, cmp::Ordering, convert::TryFrom, fmt::Debug},
    thiserror::Error,
    Literal::*,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum LiteralError {
    #[error("unsupported literal binary arithmetic between {0} and {1}")]
    UnsupportedBinaryArithmetic(String, String),

    #[error("the divisor should not be zero")]
    DivisorShouldNotBeZero,

    #[error("literal unary operation on non-numeric")]
    UnaryOperationOnNonNumeric,

    #[error("unreachable literal binary arithmetic")]
    UnreachableBinaryArithmetic,

    #[error("unreachable literal unary operation")]
    UnreachableUnaryOperation,

    #[error("failed to decode hex string: {0}")]
    FailedToDecodeHexString(String),

    #[error("operator doesn't exist: {base:?} {case} {pattern:?}", case = if *case_sensitive { "LIKE" } else { "ILIKE" })]
    LikeOnNonString {
        base: String,
        pattern: String,
        case_sensitive: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal<'a> {
    Boolean(bool),
    Number(Cow<'a, BigDecimal>),
    Text(Cow<'a, str>),
    Bytea(Vec<u8>),
    Null,
}

impl<'a> TryFrom<&'a AstLiteral> for Literal<'a> {
    type Error = Error;

    fn try_from(ast_literal: &'a AstLiteral) -> Result<Self> {
        let literal = match ast_literal {
            AstLiteral::Boolean(v) => Boolean(*v),
            AstLiteral::Number(v) => Number(Cow::Borrowed(v)),
            AstLiteral::QuotedString(v) => Text(Cow::Borrowed(v)),
            AstLiteral::HexString(v) => {
                Bytea(hex::decode(v).map_err(|_| LiteralError::FailedToDecodeHexString(v.clone()))?)
            }
            AstLiteral::Null => Null,
        };

        Ok(literal)
    }
}

impl<'a> Literal<'a> {
    pub fn evaluate_eq(&self, other: &Literal<'_>) -> bool {
        match (self, other) {
            (Null, Null) => false,
            _ => self == other,
        }
    }

    pub fn evaluate_cmp(&self, other: &Literal<'a>) -> Option<Ordering> {
        match (self, other) {
            (Boolean(l), Boolean(r)) => Some(l.cmp(r)),
            (Number(l), Number(r)) => Some(l.cmp(r)),
            (Text(l), Text(r)) => Some(l.cmp(r)),
            (Bytea(l), Bytea(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }

    pub fn unary_plus(&self) -> Result<Self> {
        match self {
            Number(v) => Ok(Number(v.clone())),
            Null => Ok(Null),
            _ => Err(LiteralError::UnaryOperationOnNonNumeric.into()),
        }
    }

    pub fn unary_minus(&self) -> Result<Self> {
        match self {
            Number(v) => Ok(Number(Cow::Owned(-v.as_ref()))),
            Null => Ok(Null),
            _ => Err(LiteralError::UnaryOperationOnNonNumeric.into()),
        }
    }

    pub fn concat(self, other: Literal<'_>) -> Self {
        let convert = |literal| match literal {
            Boolean(v) => Some(if v {
                "TRUE".to_owned()
            } else {
                "FALSE".to_owned()
            }),
            Number(v) => Some(v.to_string()),
            Text(v) => Some(v.into_owned()),
            Bytea(_) | Null => None,
        };

        match (convert(self), convert(other)) {
            (Some(l), Some(r)) => Literal::Text(Cow::Owned(l + &r)),
            _ => Literal::Null,
        }
    }

    pub fn add(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => Ok(Number(Cow::Owned(l.as_ref() + r.as_ref()))),
            (Null, Number(_)) | (Number(_), Null) | (Null, Null) => Ok(Literal::Null),
            _ => Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", self),
                format!("{:?}", other),
            )
            .into()),
        }
    }

    pub fn subtract(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => Ok(Number(Cow::Owned(l.as_ref() - r.as_ref()))),
            (Null, Number(_)) | (Number(_), Null) | (Null, Null) => Ok(Literal::Null),
            _ => Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", self),
                format!("{:?}", other),
            )
            .into()),
        }
    }

    pub fn multiply(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => Ok(Number(Cow::Owned(l.as_ref() * r.as_ref()))),
            (Null, Number(_)) | (Number(_), Null) | (Null, Null) => Ok(Literal::Null),
            _ => Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", self),
                format!("{:?}", other),
            )
            .into()),
        }
    }

    pub fn divide(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => {
                if *r.as_ref() == 0.into() {
                    Err(LiteralError::DivisorShouldNotBeZero.into())
                } else {
                    Ok(Number(Cow::Owned(l.as_ref() / r.as_ref())))
                }
            }
            (Null, Number(_)) | (Number(_), Null) | (Null, Null) => Ok(Literal::Null),
            _ => Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", self),
                format!("{:?}", other),
            )
            .into()),
        }
    }

    pub fn modulo(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => {
                if *r.as_ref() == 0.into() {
                    Err(LiteralError::DivisorShouldNotBeZero.into())
                } else {
                    Ok(Number(Cow::Owned(l.as_ref() % r.as_ref())))
                }
            }
            (Null, Number(_)) | (Number(_), Null) | (Null, Null) => Ok(Literal::Null),
            _ => Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", self),
                format!("{:?}", other),
            )
            .into()),
        }
    }

    pub fn like(&self, other: &Literal<'a>, case_sensitive: bool) -> Result<Self> {
        match (self, other) {
            (Text(l), Text(r)) => l.like(r, case_sensitive).map(Boolean),
            _ => Err(LiteralError::LikeOnNonString {
                base: format!("{:?}", self),
                pattern: format!("{:?}", other),
                case_sensitive,
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Literal::*,
        bigdecimal::BigDecimal,
        std::{borrow::Cow, str::FromStr},
    };

    #[test]
    fn try_from_ast_literal() {
        use {
            super::{Literal, LiteralError},
            crate::{ast::AstLiteral, result::Result},
        };

        fn test(ast_literal: AstLiteral, literal: Result<Literal>) {
            assert_eq!((&ast_literal).try_into(), literal);
        }

        test(AstLiteral::Boolean(true), Ok(Boolean(true)));
        test(
            AstLiteral::Number(BigDecimal::from(123)),
            Ok(Number(Cow::Borrowed(&BigDecimal::from(123)))),
        );
        test(
            AstLiteral::QuotedString("abc".to_owned()),
            Ok(Text(Cow::Borrowed("abc"))),
        );
        test(
            AstLiteral::HexString("1A2B".to_owned()),
            Ok(Bytea(hex::decode("1A2B").unwrap())),
        );
        test(
            AstLiteral::HexString("!*@Q".to_owned()),
            Err(LiteralError::FailedToDecodeHexString("!*@Q".to_owned()).into()),
        );
        assert_eq!(Literal::try_from(&AstLiteral::Null).unwrap(), Null);
    }

    #[test]
    fn arithmetic() {
        use crate::data::LiteralError;

        let num = |n: i32| Number(Cow::Owned(BigDecimal::from(n)));

        assert_eq!(Null.add(&num(1)), Ok(Null));
        assert_eq!(num(1).add(&Null), Ok(Null));

        // subtract test
        assert_eq!(Null.subtract(&num(2)), Ok(Null));
        assert_eq!(num(2).subtract(&Null), Ok(Null));
        assert_eq!(Null.subtract(&Null), Ok(Null));
        assert_eq!(
            Boolean(true).subtract(&num(3)),
            Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", Boolean(true)),
                format!("{:?}", num(3)),
            )
            .into()),
        );

        // multiply test
        assert_eq!(Null.multiply(&num(2)), Ok(Null));
        assert_eq!(num(2).multiply(&Null), Ok(Null));
        assert_eq!(Null.multiply(&Null), Ok(Null));
        assert_eq!(
            Boolean(true).multiply(&num(3)),
            Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", Boolean(true)),
                format!("{:?}", num(3)),
            )
            .into()),
        );

        assert_eq!(num(2).unary_plus(), Ok(num(2)));
        assert_eq!(Null.unary_plus(), Ok(Null));
        assert_eq!(num(1).unary_minus(), Ok(num(-1)));
        assert_eq!(Null.unary_minus(), Ok(Null));
    }

    #[test]
    fn concat() {
        macro_rules! text {
            ($text: expr) => {
                Text(Cow::Owned($text.to_owned()))
            };
        }

        let num = || Number(Cow::Owned(BigDecimal::from(123)));
        let text = || text!("Foo");

        assert_eq!(Boolean(true).concat(num()), text!("TRUE123"));
        assert_eq!(Boolean(false).concat(text()), text!("FALSEFoo"));
        assert_eq!(num().concat(num()), text!("123123"));
        assert_eq!(text().concat(num()), text!("Foo123"));
        assert_eq!(text().concat(Null), Null);
        assert_eq!(Null.concat(Boolean(true)), Null);
        assert_eq!(Null.concat(Null), Null);
    }

    #[test]
    fn div_mod() {
        use crate::data::LiteralError;

        macro_rules! num {
            ($num: expr) => {
                Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }

        let num_divisor = |x| Number(Cow::Owned(BigDecimal::from_str(x).unwrap()));

        // Divide Test
        assert_eq!(num!("12").divide(&num_divisor("2")).unwrap(), num!("6"));
        assert_eq!(num!("12").divide(&num_divisor("2.0")).unwrap(), num!("6"));
        assert_eq!(num!("12.0").divide(&num_divisor("2")).unwrap(), num!("6"));
        assert_eq!(num!("12.0").divide(&num_divisor("2.0")).unwrap(), num!("6"));
        assert_eq!(num!("12").divide(&Null).unwrap(), Null);
        assert_eq!(num!("12.5").divide(&Null).unwrap(), Null);
        assert_eq!(Null.divide(&num_divisor("2")).unwrap(), Null);
        assert_eq!(Null.divide(&num_divisor("2.5")).unwrap(), Null);
        assert_eq!(Null.divide(&Null).unwrap(), Null);
        assert_eq!(
            Boolean(true).divide(&num_divisor("3")),
            Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", Boolean(true)),
                format!("{:?}", num!("3")),
            )
            .into()),
        );

        // Modulo Test
        assert_eq!(num!("12").modulo(&num_divisor("2")).unwrap(), num!("0"));
        assert_eq!(num!("12").modulo(&num_divisor("2.0")).unwrap(), num!("0"));
        assert_eq!(num!("12.0").modulo(&num_divisor("2")).unwrap(), num!("0"));
        assert_eq!(num!("12.0").modulo(&num_divisor("2.0")).unwrap(), num!("0"));
        assert_eq!(num!("12").modulo(&Null).unwrap(), Null);
        assert_eq!(Null.modulo(&num_divisor("2")).unwrap(), Null);
        assert_eq!(Null.modulo(&Null).unwrap(), Null);
    }

    #[test]
    fn evaluate_eq() {
        macro_rules! text {
            ($text: expr) => {
                Text(Cow::Owned($text.to_owned()))
            };
        }
        macro_rules! num {
            ($num: expr) => {
                Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }
        macro_rules! bytea {
            ($val: expr) => {
                Bytea(hex::decode($val).unwrap())
            };
        }

        //Boolean
        assert!(Boolean(true).evaluate_eq(&Boolean(true)));
        assert!(!Boolean(true).evaluate_eq(&Boolean(false)));
        //Number
        assert!(num!("123").evaluate_eq(&num!("123")));
        assert!(num!("12.0").evaluate_eq(&num!("12.0")));
        assert!(num!("12.0").evaluate_eq(&num!("12")));
        assert!(!num!("12.0").evaluate_eq(&num!("12.123")));
        assert!(!num!("123").evaluate_eq(&num!("12.3")));
        assert!(!num!("123").evaluate_eq(&text!("Foo")));
        assert!(!num!("123").evaluate_eq(&Null));
        //Text
        assert!(text!("Foo").evaluate_eq(&text!("Foo")));
        assert!(!text!("Foo").evaluate_eq(&text!("Bar")));
        assert!(!text!("Foo").evaluate_eq(&Null));
        //Bytea
        assert!(bytea!("12A456").evaluate_eq(&bytea!("12A456")));
        assert!(!bytea!("1230").evaluate_eq(&num!("1230")));
        assert!(!bytea!("12").evaluate_eq(&Null));
        // Null
        assert!(!Null.evaluate_eq(&Null));
    }

    #[test]
    fn evaluate_cmp() {
        use std::cmp::Ordering;
        macro_rules! text {
            ($text: expr) => {
                Text(Cow::Owned($text.to_owned()))
            };
        }
        macro_rules! num {
            ($num: expr) => {
                Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }
        macro_rules! bytea {
            ($val: expr) => {
                Bytea(hex::decode($val).unwrap())
            };
        }

        //Boolean
        assert_eq!(
            Boolean(false).evaluate_cmp(&Boolean(true)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Boolean(true).evaluate_cmp(&Boolean(true)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            Boolean(true).evaluate_cmp(&Boolean(false)),
            Some(Ordering::Greater)
        );
        assert_eq!(Boolean(true).evaluate_cmp(&num!("1")), None);
        assert_eq!(Boolean(true).evaluate_cmp(&text!("Foo")), None);
        assert_eq!(Boolean(true).evaluate_cmp(&Null), None);
        //Number - valid format -> (int, int), (float, int), (int, float), (float, float)
        assert_eq!(
            num!("123").evaluate_cmp(&num!("1234")),
            Some(Ordering::Less)
        );
        assert_eq!(
            num!("12.0").evaluate_cmp(&num!("123")),
            Some(Ordering::Less)
        );
        assert_eq!(
            num!("123").evaluate_cmp(&num!("123.1")),
            Some(Ordering::Less)
        );
        assert_eq!(
            num!("12.0").evaluate_cmp(&num!("12.1")),
            Some(Ordering::Less)
        );
        assert_eq!(
            num!("123").evaluate_cmp(&num!("123")),
            Some(Ordering::Equal)
        );
        assert_eq!(
            num!("1234").evaluate_cmp(&num!("123")),
            Some(Ordering::Greater)
        );
        assert_eq!(num!("123").evaluate_cmp(&text!("123")), None);
        assert_eq!(num!("123").evaluate_cmp(&Null), None);
        //text
        assert_eq!(text!("a").evaluate_cmp(&text!("b")), Some(Ordering::Less));
        assert_eq!(text!("a").evaluate_cmp(&text!("a")), Some(Ordering::Equal));
        assert_eq!(
            text!("b").evaluate_cmp(&text!("a")),
            Some(Ordering::Greater)
        );
        assert_eq!(text!("a").evaluate_cmp(&Null), None);
        //Bytea
        assert_eq!(
            bytea!("12").evaluate_cmp(&bytea!("20")),
            Some(Ordering::Less)
        );
        assert_eq!(
            bytea!("31").evaluate_cmp(&bytea!("31")),
            Some(Ordering::Equal)
        );
        assert_eq!(
            bytea!("9A").evaluate_cmp(&bytea!("2A")),
            Some(Ordering::Greater)
        );
        assert_eq!(bytea!("345D").evaluate_cmp(&Null), None);
        assert_eq!(Null.evaluate_cmp(&Null), None);
    }
}
