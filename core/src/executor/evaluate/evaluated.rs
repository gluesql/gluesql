use {
    super::error::EvaluateError,
    crate::{
        ast::DataType,
        data::{Key, Literal, Value},
        result::{Error, Result},
    },
    std::cmp::Ordering,
};

#[derive(Clone)]
pub enum Evaluated<'a> {
    Literal(Literal<'a>),
    Value(Value),
}

impl<'a> From<Value> for Evaluated<'a> {
    fn from(value: Value) -> Self {
        Evaluated::Value(value)
    }
}

impl TryFrom<Evaluated<'_>> for Value {
    type Error = Error;

    fn try_from(e: Evaluated<'_>) -> Result<Value> {
        match e {
            Evaluated::Literal(v) => Value::try_from(v),
            Evaluated::Value(v) => Ok(v),
        }
    }
}

impl TryFrom<Evaluated<'_>> for Key {
    type Error = Error;

    fn try_from(evaluated: Evaluated<'_>) -> Result<Self> {
        Self::try_from(&evaluated)
    }
}

impl TryFrom<&Evaluated<'_>> for Key {
    type Error = Error;

    fn try_from(evaluated: &Evaluated<'_>) -> Result<Self> {
        match evaluated {
            Evaluated::Literal(l) => Value::try_from(l)?.try_into(),
            Evaluated::Value(v) => v.try_into(),
        }
    }
}

impl TryFrom<Evaluated<'_>> for bool {
    type Error = Error;

    fn try_from(e: Evaluated<'_>) -> Result<bool> {
        match e {
            Evaluated::Literal(Literal::Boolean(v)) => Ok(v),
            Evaluated::Literal(v) => {
                Err(EvaluateError::BooleanTypeRequired(format!("{:?}", v)).into())
            }
            Evaluated::Value(Value::Bool(v)) => Ok(v),
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
            | (Evaluated::Value(a), Evaluated::Literal(b)) => a == b,
            (Evaluated::Value(a), Evaluated::Value(b)) => a == b,
        }
    }
}

impl<'a> PartialOrd for Evaluated<'a> {
    fn partial_cmp(&self, other: &Evaluated<'a>) -> Option<Ordering> {
        match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => l.partial_cmp(r),
            (Evaluated::Literal(l), Evaluated::Value(r)) => r.partial_cmp(l).map(|o| o.reverse()),
            (Evaluated::Value(l), Evaluated::Literal(r)) => l.partial_cmp(r),
            (Evaluated::Value(l), Evaluated::Value(r)) => l.partial_cmp(r),
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
            value_op(&Value::try_from(l)?, r).map(Evaluated::from)
        }
        (Evaluated::Value(l), Evaluated::Literal(r)) => {
            value_op(l, &Value::try_from(r)?).map(Evaluated::from)
        }
        (Evaluated::Value(l), Evaluated::Value(r)) => value_op(l, r).map(Evaluated::from),
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

    pub fn modulo<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(self, other, |l, r| l.modulo(r), |l, r| l.modulo(r))
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

    pub fn unary_factorial(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(v) => Value::try_from(v).and_then(|v| v.unary_factorial()),
            Evaluated::Value(v) => v.unary_factorial(),
        }
        .map(Evaluated::from)
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
                Evaluated::from((Value::try_from(l)?).concat(r))
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => {
                Evaluated::from(l.concat(Value::try_from(r)?))
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => Evaluated::from(l.concat(r)),
        };

        Ok(evaluated)
    }

    pub fn like(&self, other: Evaluated<'a>, case_sensitive: bool) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => {
                Evaluated::Literal(l.like(&r, case_sensitive)?)
            }
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                Evaluated::from((Value::try_from(l)?).like(&r, case_sensitive)?)
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => {
                Evaluated::from(l.like(&Value::try_from(r)?, case_sensitive)?)
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => {
                Evaluated::from(l.like(&r, case_sensitive)?)
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
            Evaluated::Value(v) => v,
            Evaluated::Literal(v) => Value::try_from_literal(data_type, &v)?,
        };

        value.validate_null(nullable)?;

        Ok(value)
    }
}
