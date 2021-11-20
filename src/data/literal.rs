use {
    super::StringExt,
    crate::{
        ast::AstLiteral,
        data::BigDecimalExt,
        result::{Error, Result},
    },
    bigdecimal::BigDecimal,
    serde::Serialize,
    std::{borrow::Cow, cmp::Ordering, convert::TryFrom, fmt::Debug},
    thiserror::Error,
    Literal::*,
};

#[derive(Error, Serialize, Debug, PartialEq)]
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

    #[error("operator doesn't exist: {0:?} LIKE {1:?}")]
    LikeOnNonString(String, String),
}

#[derive(Clone, Debug)]
pub enum Literal<'a> {
    Boolean(bool),
    Number(Cow<'a, BigDecimal>),
    Text(Cow<'a, String>),
    Interval(super::Interval),
    Null,
}

impl<'a> TryFrom<&'a AstLiteral> for Literal<'a> {
    type Error = Error;

    fn try_from(ast_literal: &'a AstLiteral) -> Result<Self> {
        let literal = match ast_literal {
            AstLiteral::Boolean(v) => Boolean(*v),
            AstLiteral::Number(v) => Number(Cow::Borrowed(v)),
            AstLiteral::QuotedString(v) => Text(Cow::Borrowed(v)),
            AstLiteral::Interval {
                value,
                leading_field,
                last_field,
                ..
            } => Interval(super::Interval::try_from_literal(
                value,
                leading_field.as_ref(),
                last_field.as_ref(),
            )?),
            AstLiteral::Null => Null,
        };

        Ok(literal)
    }
}

impl PartialEq<Literal<'_>> for Literal<'_> {
    fn eq(&self, other: &Literal) -> bool {
        match (self, other) {
            (Boolean(l), Boolean(r)) => l == r,
            (Number(l), Number(r)) => l == r,
            (Text(l), Text(r)) => l == r,
            (Interval(l), Interval(r)) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<Literal<'_>> for Literal<'_> {
    fn partial_cmp(&self, other: &Literal) -> Option<Ordering> {
        match (self, other) {
            (Boolean(l), Boolean(r)) => Some(l.cmp(r)),
            (Number(l), Number(r)) => Some(l.cmp(r)),
            (Text(l), Text(r)) => Some(l.cmp(r)),
            (Interval(l), Interval(r)) => l.partial_cmp(r),
            _ => None,
        }
    }
}

impl<'a> Literal<'a> {
    pub fn unary_plus(&self) -> Result<Self> {
        match self {
            Number(v) => Ok(Number(v.clone())),
            Interval(v) => Ok(Interval(*v)),
            Null => Ok(Null),
            _ => Err(LiteralError::UnaryOperationOnNonNumeric.into()),
        }
    }

    pub fn unary_minus(&self) -> Result<Self> {
        match self {
            Number(v) => Ok(Number(Cow::Owned(-v.as_ref()))),
            Interval(v) => Ok(Interval(v.unary_minus())),
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
            Interval(v) => Some(v.into()),
            Null => None,
        };

        match (convert(self), convert(other)) {
            (Some(l), Some(r)) => Literal::Text(Cow::Owned(l + &r)),
            _ => Literal::Null,
        }
    }

    pub fn add<'b>(&self, other: &Literal<'a>) -> Result<Literal<'b>> {
        match (self, other) {
            (Number(l), Number(r)) => Ok(Number(Cow::Owned(l.as_ref() + r.as_ref()))),
            (Interval(l), Interval(r)) => l.add(r).map(Interval),
            (Null, Number(_))
            | (Null, Interval(_))
            | (Number(_), Null)
            | (Interval(_), Null)
            | (Null, Null) => Ok(Literal::Null),
            _ => Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", self),
                format!("{:?}", other),
            )
            .into()),
        }
    }

    pub fn subtract<'b>(&self, other: &Literal<'a>) -> Result<Literal<'b>> {
        match (self, other) {
            (Number(l), Number(r)) => Ok(Number(Cow::Owned(l.as_ref() - r.as_ref()))),
            (Interval(l), Interval(r)) => l.subtract(r).map(Interval),
            (Null, Number(_))
            | (Null, Interval(_))
            | (Number(_), Null)
            | (Interval(_), Null)
            | (Null, Null) => Ok(Literal::Null),
            _ => Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", self),
                format!("{:?}", other),
            )
            .into()),
        }
    }

    pub fn multiply<'b>(&self, other: &Literal<'a>) -> Result<Literal<'b>> {
        match (self, other) {
            (Number(l), Number(r)) => Ok(Number(Cow::Owned(l.as_ref() * r.as_ref()))),
            (Number(l), Interval(r)) | (Interval(r), Number(l)) => {
                if let Some(l) = l.to_i64() {
                    Ok(Interval(l * *r))
                } else if let Some(l) = l.to_f64() {
                    Ok(Interval(l * *r))
                } else {
                    Err(LiteralError::UnreachableBinaryArithmetic.into())
                }
            }
            (Null, Number(_))
            | (Null, Interval(_))
            | (Number(_), Null)
            | (Interval(_), Null)
            | (Null, Null) => Ok(Literal::Null),
            _ => Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", self),
                format!("{:?}", other),
            )
            .into()),
        }
    }

    pub fn divide<'b>(&self, other: &Literal<'a>) -> Result<Literal<'b>> {
        match (self, other) {
            (Number(l), Number(r)) => {
                if *r.as_ref() == 0.into() {
                    Err(LiteralError::DivisorShouldNotBeZero.into())
                } else {
                    Ok(Number(Cow::Owned(l.as_ref() / r.as_ref())))
                }
            }
            (Interval(l), Number(r)) => {
                if let Some(r) = r.to_i64() {
                    match r {
                        0 => Err(LiteralError::DivisorShouldNotBeZero.into()),
                        _ => Ok(Interval(*l / r)),
                    }
                } else if let Some(r) = r.to_f64() {
                    Ok(Interval(*l / r))
                } else {
                    Err(LiteralError::UnreachableBinaryArithmetic.into())
                }
            }
            (Null, Number(_)) | (Number(_), Null) | (Interval(_), Null) | (Null, Null) => {
                Ok(Literal::Null)
            }
            _ => Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", self),
                format!("{:?}", other),
            )
            .into()),
        }
    }

    pub fn modulo<'b>(&self, other: &Literal<'a>) -> Result<Literal<'b>> {
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
            _ => Err(
                LiteralError::LikeOnNonString(format!("{:?}", self), format!("{:?}", other)).into(),
            ),
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
    fn arithmetic() {
        use crate::data::Interval as I;
        use crate::data::LiteralError;

        let mon = |n| Interval(I::months(n));
        let num = |n: i32| Number(Cow::Owned(BigDecimal::from(n)));

        assert_eq!(mon(1).add(&mon(2)), Ok(mon(3)));
        assert_eq!(mon(3).subtract(&mon(1)), Ok(mon(2)));
        assert_eq!(mon(3).multiply(&num(-4)), Ok(mon(-12)));
        assert_eq!(num(9).multiply(&mon(2)), Ok(mon(18)));

        // multiply test
        matches!(Null.multiply(&num(2)), Ok(Null));
        matches!(Null.multiply(&mon(1)), Ok(Null));
        matches!(num(2).multiply(&Null), Ok(Null));
        matches!(mon(3).multiply(&Null), Ok(Null));
        matches!(Null.multiply(&Null), Ok(Null));
        assert_eq!(
            Boolean(true).multiply(&num(3)),
            Err(LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", Boolean(true)),
                format!("{:?}", num(3)),
            )
            .into()),
        );

        assert_eq!(
            Number(Cow::Owned(BigDecimal::try_from(3.3).unwrap())).multiply(&mon(10)),
            Ok(mon(33))
        );
        assert_eq!(num(2).unary_plus(), Ok(num(2)));
        assert_eq!(mon(2).unary_plus(), Ok(mon(2)));
        matches!(Null.unary_plus(), Ok(Null));
        assert_eq!(num(1).unary_minus(), Ok(num(-1)));
        assert_eq!(mon(1).unary_minus(), Ok(mon(-1)));
        matches!(Null.unary_minus(), Ok(Null));
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
        matches!(text().concat(Null), Null);
        matches!(Null.concat(Boolean(true)), Null);
        matches!(Null.concat(Null), Null);
    }

    #[test]
    fn div_mod() {
        use crate::data::interval::Interval as I;

        macro_rules! num {
            ($num: expr) => {
                Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }

        macro_rules! itv {
            ($itv: expr) => {
                Interval(I::Microsecond($itv))
            };
        }

        let num_divisor = |x| Number(Cow::Owned(BigDecimal::from_str(x).unwrap()));

        // Divide Test
        assert_eq!(num!("12").divide(&num_divisor("2")).unwrap(), num!("6"));
        assert_eq!(num!("12").divide(&num_divisor("2.0")).unwrap(), num!("6"));
        assert_eq!(num!("12.0").divide(&num_divisor("2")).unwrap(), num!("6"));
        assert_eq!(num!("12.0").divide(&num_divisor("2.0")).unwrap(), num!("6"));
        assert_eq!(itv!(12).divide(&num_divisor("2")).unwrap(), itv!(6));
        assert_eq!(itv!(12).divide(&num_divisor("2.0")).unwrap(), itv!(6));
        matches!(num!("12").divide(&Null).unwrap(), Null);
        matches!(itv!(12).divide(&Null).unwrap(), Null);
        matches!(Null.divide(&num_divisor("2")).unwrap(), Null);
        matches!(Null.divide(&Null).unwrap(), Null);
        // Modulo Test
        assert_eq!(num!("12").modulo(&num_divisor("2")).unwrap(), num!("0"));
        assert_eq!(num!("12").modulo(&num_divisor("2.0")).unwrap(), num!("0"));
        assert_eq!(num!("12.0").modulo(&num_divisor("2")).unwrap(), num!("0"));
        assert_eq!(num!("12.0").modulo(&num_divisor("2.0")).unwrap(), num!("0"));
        matches!(num!("12").modulo(&Null).unwrap(), Null);
        matches!(Null.modulo(&num_divisor("2")).unwrap(), Null);
        matches!(Null.modulo(&Null).unwrap(), Null);
    }
    #[test]
    fn partial_eq() {
        use crate::data::interval::Interval as I;
        macro_rules! text {
            ($text: expr) => {
                Text(Cow::Owned($text.to_owned()))
            };
        }
        macro_rules! itv {
            ($itv: expr) => {
                Interval(I::Microsecond($itv))
            };
        }
        macro_rules! num {
            ($num: expr) => {
                Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }
        //Boolean
        assert_eq!(Boolean(true), Boolean(true));
        assert!(Boolean(true) != Boolean(false));
        //Number
        assert_eq!(num!("123"), num!("123"));
        assert_eq!(num!("12.0"), num!("12.0"));
        assert!(num!("12.0") == num!("12"));
        assert!(num!("12.0") != num!("12.123"));
        assert!(num!("123") != num!("12.3"));
        assert!(num!("123") != text!("Foo"));
        assert!(num!("123") != itv!(123)); //only same data type allowed
        assert!(num!("123") != Null);
        //Text
        assert_eq!(text!("Foo"), text!("Foo"));
        assert!(text!("Foo") != text!("Bar"));
        assert!(text!("Foo") != itv!(12));
        assert!(text!("Foo") != Null);
        //Interval
        assert_eq!(itv!(123), itv!(123));
        assert!(itv!(123) != itv!(1234));
        assert!(itv!(123) != Null);
    }
    #[test]
    fn partial_ord() {
        use crate::data::interval::Interval as I;
        use std::cmp::Ordering;
        macro_rules! text {
            ($text: expr) => {
                Text(Cow::Owned($text.to_owned()))
            };
        }
        macro_rules! itv {
            ($itv: expr) => {
                Interval(I::Microsecond($itv))
            };
        }
        macro_rules! num {
            ($num: expr) => {
                Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }
        //Boolean
        assert_eq!(
            Boolean(false).partial_cmp(&Boolean(true)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Boolean(true).partial_cmp(&Boolean(true)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            Boolean(true).partial_cmp(&Boolean(false)),
            Some(Ordering::Greater)
        );
        assert_eq!(Boolean(true).partial_cmp(&num!("1")), None);
        assert_eq!(Boolean(true).partial_cmp(&text!("Foo")), None);
        assert_eq!(Boolean(true).partial_cmp(&itv!(12)), None);
        assert_eq!(Boolean(true).partial_cmp(&Null), None);
        //Number - valid format -> (int, int), (float, int), (int, float), (float, float)
        assert_eq!(num!("123").partial_cmp(&num!("1234")), Some(Ordering::Less));
        assert_eq!(num!("12.0").partial_cmp(&num!("123")), Some(Ordering::Less));
        assert_eq!(
            num!("123").partial_cmp(&num!("123.1")),
            Some(Ordering::Less)
        );
        assert_eq!(
            num!("12.0").partial_cmp(&num!("12.1")),
            Some(Ordering::Less)
        );
        assert_eq!(num!("123").partial_cmp(&num!("123")), Some(Ordering::Equal));
        assert_eq!(
            num!("1234").partial_cmp(&num!("123")),
            Some(Ordering::Greater)
        );
        assert_eq!(num!("123").partial_cmp(&text!("123")), None);
        assert_eq!(num!("123").partial_cmp(&itv!(123)), None);
        assert_eq!(num!("123").partial_cmp(&Null), None);
        //text
        assert_eq!(text!("a").partial_cmp(&text!("b")), Some(Ordering::Less));
        assert_eq!(text!("a").partial_cmp(&text!("a")), Some(Ordering::Equal));
        assert_eq!(text!("b").partial_cmp(&text!("a")), Some(Ordering::Greater));
        assert_eq!(text!("a").partial_cmp(&itv!(1)), None);
        assert_eq!(text!("a").partial_cmp(&Null), None);
        //Interval
        assert_eq!(itv!(1).partial_cmp(&itv!(2)), Some(Ordering::Less));
        assert_eq!(itv!(1).partial_cmp(&itv!(1)), Some(Ordering::Equal));
        assert_eq!(itv!(2).partial_cmp(&itv!(1)), Some(Ordering::Greater));
        assert_eq!(itv!(2).partial_cmp(&Null), None);
        assert_eq!(Null.partial_cmp(&Null), None);
    }
}
