use {
    super::EvaluateError,
    crate::{
        data::value::{TryFromLiteral, Value},
        result::{Error, Result},
    },
    sqlparser::ast::{DataType, Value as Literal},
    std::{
        borrow::Cow,
        cmp::Ordering,
        convert::{TryFrom, TryInto},
    },
};

#[derive(Clone)]
pub enum Evaluated<'a> {
    Literal(Cow<'a, Literal>),
    Value(Cow<'a, Value>),
}

impl<'a> From<Value> for Evaluated<'a> {
    fn from(value: Value) -> Self {
        Evaluated::Value(Cow::Owned(value))
    }
}

impl<'a> From<&'a Value> for Evaluated<'a> {
    fn from(value: &'a Value) -> Self {
        Evaluated::Value(Cow::Borrowed(value))
    }
}

impl<'a> From<Literal> for Evaluated<'a> {
    fn from(literal: Literal) -> Self {
        Evaluated::Literal(Cow::Owned(literal))
    }
}

impl<'a> From<&'a Literal> for Evaluated<'a> {
    fn from(literal: &'a Literal) -> Self {
        Evaluated::Literal(Cow::Borrowed(literal))
    }
}

impl TryInto<Value> for Evaluated<'_> {
    type Error = Error;

    fn try_into(self) -> Result<Value> {
        match self {
            Evaluated::Literal(v) => Value::try_from(v.as_ref()),
            Evaluated::Value(v) => Ok(v.into_owned()),
        }
    }
}

impl<'a> PartialEq for Evaluated<'a> {
    fn eq(&self, other: &Evaluated<'a>) -> bool {
        match (self, other) {
            (Evaluated::Literal(a), Evaluated::Literal(b)) => a == b,
            (Evaluated::Literal(b), Evaluated::Value(a))
            | (Evaluated::Value(a), Evaluated::Literal(b)) => a == b,
            (Evaluated::Value(a), Evaluated::Value(b)) => a == b,
        }
    }
}

impl<'a> PartialOrd for Evaluated<'a> {
    fn partial_cmp(&self, other: &Evaluated<'a>) -> Option<Ordering> {
        match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => {
                literal_partial_cmp(l.as_ref(), r.as_ref())
            }
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                r.as_ref().partial_cmp(l.as_ref()).map(|o| o.reverse())
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => l.as_ref().partial_cmp(r.as_ref()),
            (Evaluated::Value(l), Evaluated::Value(r)) => l.as_ref().partial_cmp(r.as_ref()),
        }
    }
}

fn literal_partial_cmp(l: &Literal, r: &Literal) -> Option<Ordering> {
    match (l, r) {
        (Literal::Number(l, false), Literal::Number(r, false)) => {
            match (l.parse::<i64>(), r.parse::<i64>()) {
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
            }
        }
        (Literal::SingleQuotedString(l), Literal::SingleQuotedString(r)) => Some(l.cmp(r)),
        _ => None,
    }
}

macro_rules! binary_op {
    ($name:ident, $op:tt) => {
        pub fn $name<'b>(&self, other: &Evaluated<'a>) -> Result<Evaluated<'b>> {
            let literal_binary_op = |l: &Literal, r: &Literal| match (l, r) {
                (Literal::Number(l, false), Literal::Number(r, false)) => {
                    match (l.parse::<i64>(), r.parse::<i64>()) {
                        (Ok(l), Ok(r)) => Ok(Literal::Number((l $op r).to_string(), false)),
                        (Ok(l), _) => match r.parse::<f64>() {
                            Ok(r) => Ok(Literal::Number(((l as f64) $op r).to_string(), false)),
                            _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
                        },
                        (_, Ok(r)) => match l.parse::<f64>() {
                            Ok(l) => Ok(Literal::Number((l $op (r as f64)).to_string(), false)),
                            _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
                        },
                        (_, _) => match (l.parse::<f64>(), r.parse::<f64>()) {
                            (Ok(l), Ok(r)) => Ok(Literal::Number((l $op r).to_string(), false)),
                            _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
                        },
                    }.map(Evaluated::from)
                }
                (Literal::Null, Literal::Number(_, false))
                | (Literal::Number(_, false), Literal::Null)
                | (Literal::Null, Literal::Null) => {
                    Ok(Evaluated::from(Literal::Null))
                }
                _ => Err(
                    EvaluateError::UnsupportedLiteralBinaryArithmetic(
                        l.to_string(),
                        r.to_string()
                    ).into()
                ),
            };

            let value_binary_op = |l: &Value, r: &Value| {
                l.$name(r).map(Evaluated::from)
            };

            match (self, other) {
                (Evaluated::Literal(l), Evaluated::Literal(r)) => {
                    literal_binary_op(l.as_ref(), r.as_ref())
                }
                (Evaluated::Literal(l), Evaluated::Value(r)) => {
                    value_binary_op(&Value::try_from(l.as_ref())?, r.as_ref())
                }
                (Evaluated::Value(l), Evaluated::Literal(r)) => {
                    value_binary_op(l.as_ref(), &Value::try_from(r.as_ref())?)
                }
                (Evaluated::Value(l), Evaluated::Value(r)) => {
                    value_binary_op(l.as_ref(), r.as_ref())
                }
            }
        }
    }
}

impl<'a> Evaluated<'a> {
    binary_op!(add, +);
    binary_op!(subtract, -);
    binary_op!(multiply, *);
    binary_op!(divide, /);

    pub fn unary_plus(&self) -> Result<Evaluated<'a>> {
        let unary_plus = |v: &Literal| match v {
            Literal::Number(v, false) => v
                .parse::<i64>()
                .map_or_else(
                    |_| v.parse::<f64>().map(|_| self.to_owned()),
                    |_| Ok(self.to_owned()),
                )
                .map_err(|_| EvaluateError::LiteralUnaryOperationOnNonNumeric.into()),
            Literal::Null => Ok(Evaluated::from(Literal::Null)),
            _ => Err(EvaluateError::LiteralUnaryOperationOnNonNumeric.into()),
        };

        match self {
            Evaluated::Literal(v) => unary_plus(&v),
            Evaluated::Value(v) => v.unary_plus().map(Evaluated::from),
        }
    }

    pub fn unary_minus(&self) -> Result<Evaluated<'a>> {
        let unary_minus = |v: &Literal| match v {
            Literal::Number(v, false) => v
                .parse::<i64>()
                .map_or_else(
                    |_| {
                        v.parse::<f64>()
                            .map(|v| Literal::Number((-v).to_string(), false))
                    },
                    |v| Ok(Literal::Number((-v).to_string(), false)),
                )
                .map_err(|_| EvaluateError::LiteralUnaryOperationOnNonNumeric.into()),
            Literal::Null => Ok(Literal::Null),
            _ => Err(EvaluateError::LiteralUnaryOperationOnNonNumeric.into()),
        };

        match self {
            Evaluated::Literal(v) => unary_minus(&v).map(Evaluated::from),
            Evaluated::Value(v) => v.unary_minus().map(Evaluated::from),
        }
    }

    pub fn cast(self, data_type: &DataType) -> Result<Evaluated<'a>> {
        let cast_literal = |literal: &Literal| Value::try_from_literal(data_type, literal);
        let cast_value = |value: &Value| value.cast(data_type);

        match self {
            Evaluated::Literal(value) => cast_literal(&value),
            Evaluated::Value(value) => cast_value(&value),
        }
        .map(Evaluated::from)
    }

    pub fn is_some(&self) -> bool {
        match self {
            Evaluated::Value(v) => v.is_some(),
            Evaluated::Literal(v) => v.as_ref() != &Literal::Null,
        }
    }
}
