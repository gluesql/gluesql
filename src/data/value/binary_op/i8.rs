use {
    super::TryBinaryOperator,
    crate::{data::ValueError, prelude::Value, result::Result},
    std::cmp::Ordering,
    Value::*,
};

impl PartialEq<Value> for i8 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => self == other,
            I64(other) => &(*self as i64) == other,
            F64(other) => &(*self as f64) == other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for i8 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match other {
            I8(other) => self.partial_cmp(other),
            I64(other) => (*self as i64).partial_cmp(other),
            F64(other) => (*self as f64).partial_cmp(other),
            _ => None,
        }
    }
}

impl TryBinaryOperator for i8 {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I8(rhs),
                        operator: '+',
                    }
                    .into()
                })
                .map(I8),
            I64(rhs) => Ok(I64(lhs as i64 + rhs)),
            F64(rhs) => Ok(F64(lhs as f64 + rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::AddOnNonNumeric(I8(lhs), rhs.clone()).into()),
        }
    }

    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I8(rhs),
                        operator: '-',
                    }
                    .into()
                })
                .map(I8),
            I64(rhs) => Ok(I64(lhs as i64 - rhs)),
            F64(rhs) => Ok(F64(lhs as f64 - rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::SubtractOnNonNumeric(I8(lhs), rhs.clone()).into()),
        }
    }

    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I8(rhs),
                        operator: '*',
                    }
                    .into()
                })
                .map(I8),
            I64(rhs) => Ok(I64(lhs as i64 * rhs)),
            F64(rhs) => Ok(F64(lhs as f64 * rhs)),
            Interval(rhs) => Ok(Interval(lhs * rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::MultiplyOnNonNumeric(I8(lhs), rhs.clone()).into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I8(rhs),
                        operator: '/',
                    }
                    .into()
                })
                .map(I8),
            I64(rhs) => Ok(I64(lhs as i64 / rhs)),
            F64(rhs) => Ok(F64(lhs as f64 / rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::DivideOnNonNumeric(I8(lhs), rhs.clone()).into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(I8(lhs % rhs)),
            I64(rhs) => Ok(I64(lhs as i64 % rhs)),
            F64(rhs) => Ok(F64(lhs as f64 % rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::ModuloOnNonNumeric(I8(lhs), rhs.clone()).into()),
        }
    }
}

impl PartialEq<Value> for i64 {
    fn eq(&self, other: &Value) -> bool {
        let lhs = *self;

        match *other {
            I8(rhs) => lhs == rhs as i64,
            I64(rhs) => lhs == rhs,
            F64(rhs) => lhs as f64 == rhs,
            _ => false,
        }
    }
}
