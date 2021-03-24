use {
    crate::{
        data::value::TryFromLiteral,
        data::{Literal, Value},
        result::{Error, Result},
    },
    sqlparser::ast::DataType,
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

macro_rules! binary_op {
    ($name:ident, $op:tt) => {
        pub fn $name<'b>(&self, other: &Evaluated<'a>) -> Result<Evaluated<'b>> {
            let value_binary_op = |l: &Value, r: &Value| l.$name(r).map(Evaluated::from);

            match (self, other) {
                (Evaluated::Literal(l), Evaluated::Literal(r)) => {
                    l.$name(r).map(Evaluated::Literal)
                }
                (Evaluated::Literal(l), Evaluated::Value(r)) => {
                    value_binary_op(&Value::try_from(l)?, r.as_ref())
                }
                (Evaluated::Value(l), Evaluated::Literal(r)) => {
                    value_binary_op(l.as_ref(), &Value::try_from(r)?)
                }
                (Evaluated::Value(l), Evaluated::Value(r)) => {
                    value_binary_op(l.as_ref(), r.as_ref())
                }
            }
        }
    };
}

impl<'a> Evaluated<'a> {
    binary_op!(add, +);
    binary_op!(subtract, -);
    binary_op!(multiply, *);
    binary_op!(divide, /);

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
            Evaluated::Literal(v) => !matches!(v, &Literal::Null),
        }
    }
}
