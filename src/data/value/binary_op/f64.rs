use {
    super::TryBinaryOperator,
    crate::{data::ValueError, prelude::Value, result::Result},
    std::cmp::Ordering,
    Value::*,
};

impl PartialOrd<Value> for f64 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match *other {
            I8(rhs) => self.partial_cmp(&(rhs as f64)),
            I64(rhs) => self.partial_cmp(&(rhs as f64)),
            F64(rhs) => self.partial_cmp(&rhs),
            _ => None,
        }
    }
}

impl TryBinaryOperator for f64 {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(F64(lhs + rhs as f64)),
            I64(rhs) => Ok(F64(lhs + rhs as f64)),
            F64(rhs) => Ok(F64(lhs + rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::AddOnNonNumeric(F64(lhs), rhs.clone()).into()),
        }
    }

    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(F64(lhs - rhs as f64)),
            I64(rhs) => Ok(F64(lhs - rhs as f64)),
            F64(rhs) => Ok(F64(lhs - rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::SubtractOnNonNumeric(F64(lhs), rhs.clone()).into()),
        }
    }

    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(F64(lhs * rhs as f64)),
            I64(rhs) => Ok(F64(lhs * rhs as f64)),
            F64(rhs) => Ok(F64(lhs * rhs)),
            Interval(rhs) => Ok(Interval(lhs * rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::MultiplyOnNonNumeric(F64(lhs), rhs.clone()).into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(F64(lhs / rhs as f64)),
            I64(rhs) => Ok(F64(lhs / rhs as f64)),
            F64(rhs) => Ok(F64(lhs / rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::DivideOnNonNumeric(F64(lhs), rhs.clone()).into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(F64(lhs % rhs as f64)),
            I64(rhs) => Ok(F64(lhs % rhs as f64)),
            F64(rhs) => Ok(F64(lhs % rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::ModuloOnNonNumeric(F64(lhs), rhs.clone()).into()),
        }
    }
}
