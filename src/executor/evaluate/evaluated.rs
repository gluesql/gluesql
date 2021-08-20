use {
    super::error::EvaluateError,
    crate::{
        ast::DataType,
        data::{Literal, Value},
        result::{Error, Result},
    },
    std::{
        borrow::Cow,
        cmp::Ordering,
        convert::{TryFrom, TryInto},
    },
};

#[derive(Clone)]
pub enum Evaluated<'a> {
    Literal(Literal<'a>),
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

impl TryInto<Value> for Evaluated<'_> {
    type Error = Error;

    fn try_into(self) -> Result<Value> {
        match self {
            Evaluated::Literal(v) => Value::try_from(v),
            Evaluated::Value(v) => Ok(v.into_owned()),
        }
    }
}

impl TryInto<bool> for Evaluated<'_> {
    type Error = Error;

    fn try_into(self) -> Result<bool> {
        match self {
            Evaluated::Literal(Literal::Boolean(v)) => Ok(v),
            Evaluated::Literal(v) => {
                Err(EvaluateError::BooleanTypeRequired(format!("{:?}", v)).into())
            }
            Evaluated::Value(Cow::Owned(Value::Bool(v))) => Ok(v),
            Evaluated::Value(Cow::Borrowed(Value::Bool(v))) => Ok(*v),
            Evaluated::Value(v) => {
                Err(EvaluateError::BooleanTypeRequired(format!("{:?}", v)).into())
            }
        }
    }
}

impl<'a> PartialEq for Evaluated<'a> {
    fn eq(&self, other: &Evaluated<'a>) -> bool {
        match (self, other) {
            (Evaluated::Literal(a), Evaluated::Literal(b)) => a == b,
            (Evaluated::Literal(b), Evaluated::Value(a))
            | (Evaluated::Value(a), Evaluated::Literal(b)) => a.as_ref() == b,
            (Evaluated::Value(a), Evaluated::Value(b)) => a == b,
        }
    }
}

impl<'a> PartialOrd for Evaluated<'a> {
    fn partial_cmp(&self, other: &Evaluated<'a>) -> Option<Ordering> {
        match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => l.partial_cmp(r),
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                r.as_ref().partial_cmp(l).map(|o| o.reverse())
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => l.as_ref().partial_cmp(r),
            (Evaluated::Value(l), Evaluated::Value(r)) => l.as_ref().partial_cmp(r.as_ref()),
        }
    }
}

fn binary_op<'a, 'b, T, U>(
    l: &Evaluated<'a>,
    r: &Evaluated<'b>,
    value_op: T,
    literal_op: U,
) -> Result<Evaluated<'b>>
where
    T: FnOnce(&Value, &Value) -> Result<Value>,
    U: FnOnce(&Literal<'a>, &Literal<'b>) -> Result<Literal<'b>>,
{
    match (l, r) {
        (Evaluated::Literal(l), Evaluated::Literal(r)) => literal_op(l, r).map(Evaluated::Literal),
        (Evaluated::Literal(l), Evaluated::Value(r)) => {
            value_op(&Value::try_from(l)?, r.as_ref()).map(Evaluated::from)
        }
        (Evaluated::Value(l), Evaluated::Literal(r)) => {
            value_op(l.as_ref(), &Value::try_from(r)?).map(Evaluated::from)
        }
        (Evaluated::Value(l), Evaluated::Value(r)) => {
            value_op(l.as_ref(), r.as_ref()).map(Evaluated::from)
        }
    }
}

impl<'a> Evaluated<'a> {
    pub fn add<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(self, other, |l, r| l.add(r), |l, r| l.add(r))
    }

    pub fn subtract<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(self, other, |l, r| l.subtract(r), |l, r| l.subtract(r))
    }

    pub fn multiply<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(self, other, |l, r| l.multiply(r), |l, r| l.multiply(r))
    }

    pub fn divide<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(self, other, |l, r| l.divide(r), |l, r| l.divide(r))
    }

    pub fn unary_plus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(v) => v.unary_plus().map(Evaluated::Literal),
            Evaluated::Value(v) => v.unary_plus().map(Evaluated::from),
        }
    }

    pub fn unary_minus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(v) => v.unary_minus().map(Evaluated::Literal),
            Evaluated::Value(v) => v.unary_minus().map(Evaluated::from),
        }
    }

    pub fn cast(self, data_type: &DataType) -> Result<Evaluated<'a>> {
        let cast_literal = |literal: &Literal| Value::try_cast_from_literal(data_type, literal);
        let cast_value = |value: &Value| value.cast(data_type);

        match self {
            Evaluated::Literal(value) => cast_literal(&value),
            Evaluated::Value(value) => cast_value(&value),
        }
        .map(Evaluated::from)
    }

    pub fn concat(self, other: Evaluated) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => Evaluated::Literal(l.concat(r)),
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                Evaluated::from((&Value::try_from(l)?).concat(r.as_ref()))
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => {
                Evaluated::from(l.as_ref().concat(&Value::try_from(r)?))
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => {
                Evaluated::from(l.as_ref().concat(r.as_ref()))
            }
        };

        Ok(evaluated)
    }

    pub fn like(&self, other: Evaluated<'a>) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => Evaluated::Literal(l.like(&r)?),
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                Evaluated::from((&Value::try_from(l)?).like(r.as_ref())?)
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => {
                Evaluated::from(l.as_ref().like(&Value::try_from(r)?)?)
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => {
                Evaluated::from(l.as_ref().like(r.as_ref())?)
            }
        };

        Ok(evaluated)
    }

    pub fn is_null(&self) -> bool {
        match self {
            Evaluated::Value(v) => v.is_null(),
            Evaluated::Literal(v) => matches!(v, &Literal::Null),
        }
    }

    pub fn try_into_value(self, data_type: &DataType, nullable: bool) -> Result<Value> {
        let value = match self {
            Evaluated::Value(v) => v.into_owned(),
            Evaluated::Literal(v) => Value::try_from_literal(data_type, &v)?,
        };

        value.validate_null(nullable)?;

        Ok(value)
    }
}
