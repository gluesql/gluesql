use {
    super::{BinaryOperator, TryBinaryOperator, Value},
    crate::{data::ValueError, result::Result},
    std::cmp::Ordering,
    Value::*,
};

impl BinaryOperator for i8 {
    type Rhs = Value;

    fn eq(&self, rhs: &Self::Rhs) -> bool {
        let orig = *self;

        match *rhs {
            I8(v) => orig == v,
            I64(v) => orig as i64 == v,
            F64(v) => orig as f64 == v,
            _ => false,
        }
    }

    fn partial_cmp(&self, rhs: &Self::Rhs) -> Option<Ordering> {
        let orig = *self;

        match rhs {
            I8(v) => PartialOrd::partial_cmp(&orig, v),
            I64(v) => PartialOrd::partial_cmp(&(orig as i64), v),
            F64(v) => PartialOrd::partial_cmp(&(orig as f64), v),
            _ => None,
        }
    }
}

impl TryBinaryOperator for i8 {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(I8(orig + v)),
            I64(v) => Ok(I64(orig as i64 + v)),
            F64(v) => Ok(F64(orig as f64 + v)),
            Null => Ok(Null),
            _ => Err(ValueError::AddOnNonNumeric(I8(orig), rhs.clone()).into()),
        }
    }

    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(I8(orig - v)),
            I64(v) => Ok(I64(orig as i64 - v)),
            F64(v) => Ok(F64(orig as f64 - v)),
            Null => Ok(Null),
            _ => Err(ValueError::SubtractOnNonNumeric(I8(orig), rhs.clone()).into()),
        }
    }

    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(I8(orig * v)),
            I64(v) => Ok(I64(orig as i64 * v)),
            F64(v) => Ok(F64(orig as f64 * v)),
            Interval(v) => Ok(Interval(orig * v)),
            Null => Ok(Null),
            _ => Err(ValueError::MultiplyOnNonNumeric(I8(orig), rhs.clone()).into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(I8(orig / v)),
            I64(v) => Ok(I64(orig as i64 / v)),
            F64(v) => Ok(F64(orig as f64 / v)),
            Null => Ok(Null),
            _ => Err(ValueError::DivideOnNonNumeric(I8(orig), rhs.clone()).into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(I8(orig % v)),
            I64(v) => Ok(I64(orig as i64 % v)),
            F64(v) => Ok(F64(orig as f64 % v)),
            Null => Ok(Null),
            _ => Err(ValueError::ModuloOnNonNumeric(I8(orig), rhs.clone()).into()),
        }
    }
}

impl BinaryOperator for i64 {
    type Rhs = Value;

    fn eq(&self, rhs: &Self::Rhs) -> bool {
        let orig = *self;

        match *rhs {
            I8(v) => orig == v as i64,
            I64(v) => orig == v,
            F64(v) => orig as f64 == v,
            _ => false,
        }
    }

    fn partial_cmp(&self, rhs: &Self::Rhs) -> Option<Ordering> {
        match rhs {
            I8(v) => PartialOrd::partial_cmp(self, &(*v as i64)),
            I64(v) => PartialOrd::partial_cmp(self, v),
            F64(v) => PartialOrd::partial_cmp(&(*self as f64), v),
            _ => None,
        }
    }
}

impl TryBinaryOperator for i64 {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(I64(orig + v as i64)),
            I64(v) => Ok(I64(orig + v)),
            F64(v) => Ok(F64(orig as f64 + v)),
            Null => Ok(Null),
            _ => Err(ValueError::AddOnNonNumeric(I64(orig), rhs.clone()).into()),
        }
    }

    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(I64(orig - v as i64)),
            I64(v) => Ok(I64(orig - v)),
            F64(v) => Ok(F64(orig as f64 - v)),
            Null => Ok(Null),
            _ => Err(ValueError::SubtractOnNonNumeric(I64(orig), rhs.clone()).into()),
        }
    }

    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(I64(orig * v as i64)),
            I64(v) => Ok(I64(orig * v)),
            F64(v) => Ok(F64(orig as f64 * v)),
            Interval(v) => Ok(Interval(orig * v)),
            Null => Ok(Null),
            _ => Err(ValueError::MultiplyOnNonNumeric(I64(orig), rhs.clone()).into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(I64(orig / v as i64)),
            I64(v) => Ok(I64(orig / v)),
            F64(v) => Ok(F64(orig as f64 / v)),
            Null => Ok(Null),
            _ => Err(ValueError::DivideOnNonNumeric(I64(orig), rhs.clone()).into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(I64(orig % v as i64)),
            I64(v) => Ok(I64(orig % v)),
            F64(v) => Ok(F64(orig as f64 % v)),
            Null => Ok(Null),
            _ => Err(ValueError::ModuloOnNonNumeric(I64(orig), rhs.clone()).into()),
        }
    }
}

impl BinaryOperator for f64 {
    type Rhs = Value;

    fn eq(&self, rhs: &Self::Rhs) -> bool {
        let orig = *self;

        match *rhs {
            I8(v) => orig == v as f64,
            I64(v) => orig == v as f64,
            F64(v) => orig == v,
            _ => false,
        }
    }

    fn partial_cmp(&self, rhs: &Self::Rhs) -> Option<Ordering> {
        match *rhs {
            I8(v) => PartialOrd::partial_cmp(self, &(v as f64)),
            I64(v) => PartialOrd::partial_cmp(self, &(v as f64)),
            F64(v) => PartialOrd::partial_cmp(self, &v),
            _ => None,
        }
    }
}

impl TryBinaryOperator for f64 {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(F64(orig + v as f64)),
            I64(v) => Ok(F64(orig + v as f64)),
            F64(v) => Ok(F64(orig + v)),
            Null => Ok(Null),
            _ => Err(ValueError::AddOnNonNumeric(F64(orig), rhs.clone()).into()),
        }
    }

    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(F64(orig - v as f64)),
            I64(v) => Ok(F64(orig - v as f64)),
            F64(v) => Ok(F64(orig - v)),
            Null => Ok(Null),
            _ => Err(ValueError::SubtractOnNonNumeric(F64(orig), rhs.clone()).into()),
        }
    }

    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(F64(orig * v as f64)),
            I64(v) => Ok(F64(orig * v as f64)),
            F64(v) => Ok(F64(orig * v)),
            Interval(v) => Ok(Interval(orig * v)),
            Null => Ok(Null),
            _ => Err(ValueError::MultiplyOnNonNumeric(F64(orig), rhs.clone()).into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(F64(orig / v as f64)),
            I64(v) => Ok(F64(orig / v as f64)),
            F64(v) => Ok(F64(orig / v)),
            Null => Ok(Null),
            _ => Err(ValueError::DivideOnNonNumeric(F64(orig), rhs.clone()).into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let orig = *self;

        match *rhs {
            I8(v) => Ok(F64(orig % v as f64)),
            I64(v) => Ok(F64(orig % v as f64)),
            F64(v) => Ok(F64(orig % v)),
            Null => Ok(Null),
            _ => Err(ValueError::ModuloOnNonNumeric(F64(orig), rhs.clone()).into()),
        }
    }
}
