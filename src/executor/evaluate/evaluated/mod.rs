mod evaluated_ref;

use {
    super::EvaluateError,
    crate::{
        data,
        data::value::{TryFromLiteral, Value},
        result::{Error, Result},
    },
    sqlparser::ast::{DataType, Value as Literal},
    std::{
        borrow::Cow,
        cmp::Ordering,
        convert::{TryFrom, TryInto},
    },
    Evaluated::*,
};

use evaluated_ref::EvaluatedRef;

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

impl<'a> PartialEq for Evaluated<'a> {
    fn eq(&self, other: &Evaluated<'a>) -> bool {
        let l = EvaluatedRef::from(self);
        let r = EvaluatedRef::from(other);

        l == r
    }
}

impl<'a> PartialOrd for Evaluated<'a> {
    fn partial_cmp(&self, other: &Evaluated<'a>) -> Option<Ordering> {
        let l = EvaluatedRef::from(self);
        let r = EvaluatedRef::from(other);

        l.partial_cmp(&r)
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

impl<'a> Evaluated<'a> {
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
            Literal(v) => unary_plus(&v),
            Value(v) => v.unary_plus().map(Evaluated::from),
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
            Literal(v) => unary_minus(&v).map(Evaluated::from),
            Value(v) => v.unary_minus().map(Evaluated::from),
        }
    }

    pub fn add<'b>(&self, other: &Evaluated<'_>) -> Result<Evaluated<'b>> {
        let l = EvaluatedRef::from(self);
        let r = EvaluatedRef::from(other);

        l.add(&r)
    }

    pub fn subtract<'b>(&self, other: &Evaluated<'_>) -> Result<Evaluated<'b>> {
        let l = EvaluatedRef::from(self);
        let r = EvaluatedRef::from(other);

        l.subtract(&r)
    }

    pub fn multiply<'b>(&self, other: &Evaluated<'_>) -> Result<Evaluated<'b>> {
        let l = EvaluatedRef::from(self);
        let r = EvaluatedRef::from(other);

        l.multiply(&r)
    }

    pub fn divide<'b>(&self, other: &Evaluated<'_>) -> Result<Evaluated<'b>> {
        let l = EvaluatedRef::from(self);
        let r = EvaluatedRef::from(other);

        l.divide(&r)
    }

    pub fn cast(self, data_type: &DataType) -> Result<Evaluated<'a>> {
        let cast_literal = |literal: &Literal| Value::try_from_literal(data_type, literal);
        let cast_value = |value: &data::Value| value.cast(data_type);

        match self {
            Literal(value) => cast_literal(&value),
            Value(value) => cast_value(&value),
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
