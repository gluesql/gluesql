use {
    super::Value,
    crate::{data::ValueError, result::Result},
    std::cmp::Ordering,
    Value::*,
};

pub(crate) trait TryBinaryOperator {
    type Rhs;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value>;
}

pub(crate) trait BinaryOperator {
    type Rhs;

    fn eq(&self, rhs: &Self::Rhs) -> bool;
    fn partial_cmp(&self, rhs: &Self::Rhs) -> Option<Ordering>;
}

impl BinaryOperator for i8 {
    type Rhs = Value;

    fn eq(&self, rhs: &Self::Rhs) -> bool {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs == rhs,
            I64(rhs) => lhs as i64 == rhs,
            F64(rhs) => lhs as f64 == rhs,
            _ => false,
        }
    }

    fn partial_cmp(&self, rhs: &Self::Rhs) -> Option<Ordering> {
        let lhs = *self;

        match rhs {
            I8(rhs) => PartialOrd::partial_cmp(&lhs, rhs),
            I64(rhs) => PartialOrd::partial_cmp(&(lhs as i64), rhs),
            F64(rhs) => PartialOrd::partial_cmp(&(lhs as f64), rhs),
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

impl BinaryOperator for i64 {
    type Rhs = Value;

    fn eq(&self, rhs: &Self::Rhs) -> bool {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs == rhs as i64,
            I64(rhs) => lhs == rhs,
            F64(rhs) => lhs as f64 == rhs,
            _ => false,
        }
    }

    fn partial_cmp(&self, rhs: &Self::Rhs) -> Option<Ordering> {
        match rhs {
            I8(rhs) => PartialOrd::partial_cmp(self, &(*rhs as i64)),
            I64(rhs) => PartialOrd::partial_cmp(self, rhs),
            F64(rhs) => PartialOrd::partial_cmp(&(*self as f64), rhs),
            _ => None,
        }
    }
}

impl TryBinaryOperator for i64 {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(I64(lhs + rhs as i64)),
            I64(rhs) => Ok(I64(lhs + rhs)),
            F64(rhs) => Ok(F64(lhs as f64 + rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::AddOnNonNumeric(I64(lhs), rhs.clone()).into()),
        }
    }

    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(I64(lhs - rhs as i64)),
            I64(rhs) => Ok(I64(lhs - rhs)),
            F64(rhs) => Ok(F64(lhs as f64 - rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::SubtractOnNonNumeric(I64(lhs), rhs.clone()).into()),
        }
    }

    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(I64(lhs * rhs as i64)),
            I64(rhs) => Ok(I64(lhs * rhs)),
            F64(rhs) => Ok(F64(lhs as f64 * rhs)),
            Interval(rhs) => Ok(Interval(lhs * rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::MultiplyOnNonNumeric(I64(lhs), rhs.clone()).into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(I64(lhs / rhs as i64)),
            I64(rhs) => Ok(I64(lhs / rhs)),
            F64(rhs) => Ok(F64(lhs as f64 / rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::DivideOnNonNumeric(I64(lhs), rhs.clone()).into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(I64(lhs % rhs as i64)),
            I64(rhs) => Ok(I64(lhs % rhs)),
            F64(rhs) => Ok(F64(lhs as f64 % rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::ModuloOnNonNumeric(I64(lhs), rhs.clone()).into()),
        }
    }
}

impl BinaryOperator for f64 {
    type Rhs = Value;

    fn eq(&self, rhs: &Self::Rhs) -> bool {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs == rhs as f64,
            I64(rhs) => lhs == rhs as f64,
            F64(rhs) => lhs == rhs,
            _ => false,
        }
    }

    fn partial_cmp(&self, rhs: &Self::Rhs) -> Option<Ordering> {
        match *rhs {
            I8(rhs) => PartialOrd::partial_cmp(self, &(rhs as f64)),
            I64(rhs) => PartialOrd::partial_cmp(self, &(rhs as f64)),
            F64(rhs) => PartialOrd::partial_cmp(self, &rhs),
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
