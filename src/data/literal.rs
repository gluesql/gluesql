use {
    crate::result::{Error, Result},
    sqlparser::ast::{Expr, Ident, Value as AstValue},
    std::{borrow::Cow, cmp::Ordering, convert::TryFrom, fmt::Debug},
    Literal::*,
};

use {serde::Serialize, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum LiteralError {
    #[error("unsupported literal type: {0}")]
    UnsupportedLiteralType(String),

    #[error("unsupported expr: {0}")]
    UnsupportedExpr(String),

    #[error("unsupported literal binary arithmetic between {0} and {1}")]
    UnsupportedBinaryArithmetic(String, String),

    #[error("literal unary operation on non-numeric")]
    UnaryOperationOnNonNumeric,

    #[error("unreachable literal binary arithmetic")]
    UnreachableBinaryArithmetic,

    #[error("unreachable literal unary operation")]
    UnreachableUnaryOperation,
}

#[derive(Clone, Debug)]
pub enum Literal<'a> {
    Boolean(bool),
    Number(Cow<'a, String>),
    Text(Cow<'a, String>),
    Null,
}

impl<'a> TryFrom<&'a AstValue> for Literal<'a> {
    type Error = Error;

    fn try_from(ast_value: &'a AstValue) -> Result<Self> {
        let literal = match ast_value {
            AstValue::Boolean(v) => Boolean(*v),
            AstValue::Number(v, false) => Number(Cow::Borrowed(v)),
            AstValue::SingleQuotedString(v) => Text(Cow::Borrowed(v)),
            AstValue::Null => Null,
            _ => {
                return Err(LiteralError::UnsupportedLiteralType(ast_value.to_string()).into());
            }
        };

        Ok(literal)
    }
}

impl<'a> TryFrom<&'a Expr> for Literal<'a> {
    type Error = Error;

    fn try_from(expr: &'a Expr) -> Result<Self> {
        match expr {
            Expr::Value(literal) => Literal::try_from(literal),
            Expr::Identifier(Ident { value, .. }) => Ok(Literal::Text(Cow::Borrowed(value))),
            _ => Err(LiteralError::UnsupportedExpr(expr.to_string()).into()),
        }
    }
}

impl PartialEq<Literal<'_>> for Literal<'_> {
    fn eq(&self, other: &Literal) -> bool {
        match (self, other) {
            (Boolean(l), Boolean(r)) => l == r,
            (Number(l), Number(r)) | (Text(l), Text(r)) => l == r,
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
            _ => None,
        }
    }
}

macro_rules! binary_op {
    ($name:ident, $op:tt) => {
        pub fn $name<'b>(&self, other: &Literal<'a>) -> Result<Literal<'b>> {
            match (self, other) {
                (Number(l), Number(r)) => {
                    match (l.parse::<i64>(), r.parse::<i64>()) {
                        (Ok(l), Ok(r)) => Ok((l $op r).to_string()),
                        (Ok(l), _) => match r.parse::<f64>() {
                            Ok(r) => Ok(((l as f64) $op r).to_string()),
                            _ => Err(LiteralError::UnreachableBinaryArithmetic.into()),
                        },
                        (_, Ok(r)) => match l.parse::<f64>() {
                            Ok(l) => Ok((l $op (r as f64)).to_string()),
                            _ => Err(LiteralError::UnreachableBinaryArithmetic.into()),
                        },
                        (_, _) => match (l.parse::<f64>(), r.parse::<f64>()) {
                            (Ok(l), Ok(r)) => Ok((l $op r).to_string()),
                            _ => Err(LiteralError::UnreachableBinaryArithmetic.into()),
                        },
                    }.map(|v| Number(Cow::Owned(v)))
                }
                (Null, Number(_))
                | (Number(_), Null)
                | (Null, Null) => {
                    Ok(Literal::Null)
                }
                _ => Err(
                    LiteralError::UnsupportedBinaryArithmetic(
                        format!("{:?}", self),
                        format!("{:?}", other),
                    ).into()
                ),
            }
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
            Null => Ok(Null),
            _ => Err(LiteralError::UnaryOperationOnNonNumeric.into()),
        }
    }

    binary_op!(add, +);
    binary_op!(subtract, -);
    binary_op!(multiply, *);
    binary_op!(divide, /);
}
