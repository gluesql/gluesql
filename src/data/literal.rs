use {
    super::StringExt,
    crate::{
        ast::AstLiteral,
        result::{Error, Result},
    },
    serde::Serialize,
    std::{borrow::Cow, cmp::Ordering, convert::TryFrom, fmt::Debug},
    thiserror::Error,
    Literal::*,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum LiteralError {
    #[error("unsupported literal binary arithmetic between {0} and {1}")]
    UnsupportedBinaryArithmetic(String, String),

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
    Number(Cow<'a, String>),
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
            (Number(l), Number(r)) | (Text(l), Text(r)) => l == r,
            (Interval(l), Interval(r)) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<Literal<'_>> for Literal<'_> {
    fn partial_cmp(&self, other: &Literal) -> Option<Ordering> {
        match (self, other) {
            (Boolean(l), Boolean(r)) => Some(l.cmp(r)),
            (Number(l), Number(r)) => match (l.parse::<i64>(), r.parse::<i64>()) {
                (Ok(l), Ok(r)) => Some(l.cmp(&r)),
                (_, Ok(r)) => match l.parse::<f64>() {
                    Ok(l) => l.partial_cmp(&(r as f64)),
                    _ => None,
                },
                (Ok(l), _) => match r.parse::<f64>() {
                    Ok(r) => (l as f64).partial_cmp(&r),
                    _ => None,
                },
                _ => match (l.parse::<f64>(), r.parse::<f64>()) {
                    (Ok(l), Ok(r)) => l.partial_cmp(&r),
                    _ => None,
                },
            },
            (Text(l), Text(r)) => Some(l.cmp(r)),
            (Interval(l), Interval(r)) => l.partial_cmp(r),
            _ => None,
        }
    }
}

impl<'a> Literal<'a> {
    pub fn unary_plus(&self) -> Result<Self> {
        match self {
            Number(v) => v
                .parse::<i64>()
                .map(|_| self.to_owned())
                .or_else(|_| v.parse::<f64>().map(|_| self.to_owned()))
                .map_err(|_| LiteralError::UnreachableUnaryOperation.into()),
            Interval(v) => Ok(Interval(*v)),
            Null => Ok(Null),
            _ => Err(LiteralError::UnaryOperationOnNonNumeric.into()),
        }
    }

    pub fn unary_minus(&self) -> Result<Self> {
        match self {
            Number(v) => v
                .parse::<i64>()
                .map(|v| (-v).to_string())
                .or_else(|_| v.parse::<f64>().map(|v| (-v).to_string()))
                .map(|v| Number(Cow::Owned(v)))
                .map_err(|_| LiteralError::UnreachableUnaryOperation.into()),
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
            Number(v) => Some(v.into_owned()),
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
            (Number(l), Number(r)) => {
                if let (Ok(l), Ok(r)) = (l.parse::<i64>(), r.parse::<i64>()) {
                    Ok(Number(Cow::Owned((l + r).to_string())))
                } else if let (Ok(l), Ok(r)) = (l.parse::<f64>(), r.parse::<f64>()) {
                    Ok(Number(Cow::Owned((l + r).to_string())))
                } else {
                    Err(LiteralError::UnreachableBinaryArithmetic.into())
                }
            }
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
            (Number(l), Number(r)) => {
                if let (Ok(l), Ok(r)) = (l.parse::<i64>(), r.parse::<i64>()) {
                    Ok(Number(Cow::Owned((l - r).to_string())))
                } else if let (Ok(l), Ok(r)) = (l.parse::<f64>(), r.parse::<f64>()) {
                    Ok(Number(Cow::Owned((l - r).to_string())))
                } else {
                    Err(LiteralError::UnreachableBinaryArithmetic.into())
                }
            }
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
            (Number(l), Number(r)) => {
                if let (Ok(l), Ok(r)) = (l.parse::<i64>(), r.parse::<i64>()) {
                    Ok(Number(Cow::Owned((l * r).to_string())))
                } else if let (Ok(l), Ok(r)) = (l.parse::<f64>(), r.parse::<f64>()) {
                    Ok(Number(Cow::Owned((l * r).to_string())))
                } else {
                    Err(LiteralError::UnreachableBinaryArithmetic.into())
                }
            }
            (Number(l), Interval(r)) | (Interval(r), Number(l)) => {
                if let Ok(l) = l.parse::<i64>() {
                    Ok(Interval(l * *r))
                } else if let Ok(l) = l.parse::<f64>() {
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
                if let (Ok(l), Ok(r)) = (l.parse::<i64>(), r.parse::<i64>()) {
                    Ok(Number(Cow::Owned((l / r).to_string())))
                } else if let (Ok(l), Ok(r)) = (l.parse::<f64>(), r.parse::<f64>()) {
                    Ok(Number(Cow::Owned((l / r).to_string())))
                } else {
                    Err(LiteralError::UnreachableBinaryArithmetic.into())
                }
            }
            (Number(l), Interval(r)) => {
                if let Ok(l) = l.parse::<i64>() {
                    Ok(Interval(l / *r))
                } else if let Ok(l) = l.parse::<f64>() {
                    Ok(Interval(l / *r))
                } else {
                    Err(LiteralError::UnreachableBinaryArithmetic.into())
                }
            }
            (Interval(l), Number(r)) => {
                if let Ok(r) = r.parse::<i64>() {
                    Ok(Interval(*l / r))
                } else if let Ok(r) = r.parse::<f64>() {
                    Ok(Interval(*l / r))
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

    pub fn like(&self, other: &Literal<'a>) -> Result<Self> {
        match (self, other) {
            (Text(l), Text(r)) => l.like(&r).map(Boolean),
            _ => Err(
                LiteralError::LikeOnNonString(format!("{:?}", self), format!("{:?}", other)).into(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Literal::*;
    use std::borrow::Cow;

    #[test]
    fn arithmetic() {
        use crate::data::Interval as I;

        let mon = |n| Interval(I::months(n));
        let num = |n: i32| Number(Cow::Owned(n.to_string()));

        assert_eq!(mon(1).add(&mon(2)), Ok(mon(3)));
        assert_eq!(mon(3).subtract(&mon(1)), Ok(mon(2)));
        assert_eq!(mon(3).multiply(&num(-4)), Ok(mon(-12)));
        assert_eq!(num(9).multiply(&mon(2)), Ok(mon(18)));
        assert_eq!(mon(14).divide(&num(3)), Ok(mon(4)));
        assert_eq!(num(27).divide(&mon(9)), Ok(mon(3)));
    }

    #[test]
    fn concat() {
        macro_rules! text {
            ($text: expr) => {
                Text(Cow::Owned($text.to_owned()))
            };
        }

        let num = || Number(Cow::Owned("123".to_owned()));
        let text = || text!("Foo");

        assert_eq!(Boolean(true).concat(num()), text!("TRUE123"));
        assert_eq!(Boolean(false).concat(text()), text!("FALSEFoo"));
        assert_eq!(num().concat(num()), text!("123123"));
        assert_eq!(text().concat(num()), text!("Foo123"));
        matches!(text().concat(Null), Null);
        matches!(Null.concat(Boolean(true)), Null);
        matches!(Null.concat(Null), Null);
    }
}
