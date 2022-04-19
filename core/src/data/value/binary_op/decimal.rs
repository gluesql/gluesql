use {
    super::TryBinaryOperator,
    crate::{data::ValueError, prelude::Value, result::Result},
    rust_decimal::prelude::Decimal as Dec,
    std::cmp::Ordering,
    Value::*,
};

impl PartialEq<Value> for Dec {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => *self == Dec::from(*other),
            I64(other) => *self == Dec::from(*other),
            F64(other) => *self == Dec::from_f64_retain(*other).unwrap(),
			Decimal(other) => *self == *other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for Dec {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match *other {
            I8(rhs) => self.partial_cmp(&(Dec::from(rhs))),
            I64(rhs) => self.partial_cmp(&(Dec::from(rhs))),
            F64(rhs) => self.partial_cmp(&(Dec::from_f64_retain(rhs).unwrap())),
            Decimal(rhs) => self.partial_cmp(&rhs),
            _ => None,
        }
    }
}

impl TryBinaryOperator for Dec {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(Decimal(lhs + Dec::from(rhs))),
            I64(rhs) => Ok(Decimal(lhs + Dec::from(rhs))),
            F64(rhs) => Ok(Decimal(lhs + Dec::from_f64_retain(rhs).unwrap())),
            Decimal(rhs) => Ok(Decimal(lhs + rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::AddOnNonNumeric(Decimal(lhs), rhs.clone()).into()),
        }
    }

    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(Decimal(lhs - Dec::from(rhs))),
            I64(rhs) => Ok(Decimal(lhs - Dec::from(rhs))),
            F64(rhs) => Ok(Decimal(lhs - Dec::from_f64_retain(rhs).unwrap())),
            Decimal(rhs) => Ok(Decimal(lhs - rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::SubtractOnNonNumeric(Decimal(lhs), rhs.clone()).into()),
        }
    }

    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(Decimal(lhs * Dec::from(rhs))),
            I64(rhs) => Ok(Decimal(lhs * Dec::from(rhs))),
            F64(rhs) => Ok(Decimal(lhs * Dec::from_f64_retain(rhs).unwrap())),
            Decimal(rhs) => Ok(Decimal(lhs * rhs)),
            //Interval(rhs) => Ok(Interval(lhs * rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::MultiplyOnNonNumeric(Decimal(lhs), rhs.clone()).into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(Decimal(lhs / Dec::from(rhs))),
            I64(rhs) => Ok(Decimal(lhs / Dec::from(rhs))),
            F64(rhs) => Ok(Decimal(lhs / Dec::from_f64_retain(rhs).unwrap())),
            Decimal(rhs) => Ok(Decimal(lhs / rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::DivideOnNonNumeric(Decimal(lhs), rhs.clone()).into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(Decimal(lhs.checked_rem(Dec::from(rhs)).unwrap())),
            I64(rhs) => Ok(Decimal(lhs.checked_rem(Dec::from(rhs)).unwrap())),
            F64(rhs) => Ok(Decimal(lhs.checked_rem(Dec::from_f64_retain(rhs).unwrap()).unwrap())),
            Decimal(rhs) => Ok(Decimal(lhs.checked_rem(Dec::from(rhs)).unwrap())),
            Null => Ok(Null),
            _ => Err(ValueError::ModuloOnNonNumeric(Decimal(lhs), rhs.clone()).into()),
        }
    }
}

/*
#[cfg(test)]
mod tests {
    use {
        super::{TryBinaryOperator, Value::*},
        crate::data::ValueError,
        std::cmp::Ordering,
    };

    #[test]
    fn eq() {
        let base = 1.0_f64;

        assert_eq!(base, I8(1));
        assert_eq!(base, I64(1));
        assert_eq!(base, F64(1.0));

        assert_ne!(base, Bool(true));
    }

    #[test]
    fn partial_cmp() {
        let base = 1.0_f64;

        assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F64(1.0)), Some(Ordering::Equal));

        assert_eq!(base.partial_cmp(&Bool(true)), None);
    }

    #[test]
    fn try_add() {
        let base = 1.0_f64;

        assert!(matches!(base.try_add(&I8(1)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON ));
        assert!(matches!(base.try_add(&I64(1)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON ));
        assert!(matches!(base.try_add(&F64(1.0)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON ));

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::AddOnNonNumeric(F64(1.0), Bool(true)).into())
        );
    }

    #[test]
    fn try_subtract() {
        let base = 1.0_f64;

        assert!(matches!(base.try_subtract(&I8(1)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON ));
        assert!(
            matches!(base.try_subtract(&I64(1)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_subtract(&F64(1.0)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON )
        );

        assert_eq!(
            base.try_subtract(&Bool(true)),
            Err(ValueError::SubtractOnNonNumeric(F64(1.0), Bool(true)).into())
        );
    }

    #[test]
    fn try_multiply() {
        let base = 1.0_f64;

        assert!(matches!(base.try_multiply(&I8(1)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON ));
        assert!(
            matches!(base.try_multiply(&I64(1)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&F64(1.0)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON )
        );

        assert_eq!(
            base.try_multiply(&Bool(true)),
            Err(ValueError::MultiplyOnNonNumeric(F64(1.0), Bool(true)).into())
        );
    }

    #[test]
    fn try_divide() {
        let base = 1.0_f64;

        assert!(matches!(base.try_divide(&I8(1)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON ));
        assert!(matches!(base.try_divide(&I64(1)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON ));
        assert!(
            matches!(base.try_divide(&F64(1.0)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON )
        );

        assert_eq!(
            base.try_divide(&Bool(true)),
            Err(ValueError::DivideOnNonNumeric(F64(1.0), Bool(true)).into())
        );
    }

    #[test]
    fn try_modulo() {
        let base = 1.0_f64;

        assert!(matches!(base.try_modulo(&I8(1)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON ));
        assert!(matches!(base.try_modulo(&I64(1)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON ));
        assert!(
            matches!(base.try_modulo(&F64(1.0)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON )
        );

        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::ModuloOnNonNumeric(F64(1.0), Bool(true)).into())
        );
    }
}
*/