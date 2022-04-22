use {
    super::TryBinaryOperator,
    crate::{
        data::ValueError,
        prelude::Value,
        result::{Error, Result},
    },
    rust_decimal::prelude::Decimal as Dec,
    std::cmp::Ordering,
    Value::*,
};

impl PartialEq<Value> for Dec {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => *self == Dec::from(*other),
            I64(other) => *self == Dec::from(*other),
            F64(other) => match Dec::from_f64_retain(*other) {
                Some(x) => *self == x,
                _ => false,
            },
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
            F64(rhs) => match Dec::from_f64_retain(rhs) {
                Some(x) => self.partial_cmp(&x),
                _ => None,
            },
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
            F64(rhs) => match Dec::from_f64_retain(rhs) {
                Some(x) => Ok(Decimal(lhs + x)),
                _ => Err(Error::OverflowError("+".to_string())),
            },
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
            F64(rhs) => match Dec::from_f64_retain(rhs) {
                Some(x) => Ok(Decimal(lhs - x)),
                _ => Err(Error::OverflowError("-".to_string())),
            },

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
            F64(rhs) => match Dec::from_f64_retain(rhs) {
                Some(x) => Ok(Decimal(lhs * x)),
                _ => Err(Error::OverflowError("*".to_string())),
            },
            Decimal(rhs) => Ok(Decimal(lhs * rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::MultiplyOnNonNumeric(Decimal(lhs), rhs.clone()).into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(Decimal(lhs / Dec::from(rhs))),
            I64(rhs) => Ok(Decimal(lhs / Dec::from(rhs))),
            F64(rhs) => match Dec::from_f64_retain(rhs) {
                Some(x) => Ok(Decimal(lhs / x)),
                _ => Err(Error::OverflowError("/".to_string())),
            },

            Decimal(rhs) => Ok(Decimal(lhs / rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::DivideOnNonNumeric(Decimal(lhs), rhs.clone()).into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => match lhs.checked_rem(Dec::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(Error::OverflowError("%".to_string())),
            },
            I64(rhs) => match lhs.checked_rem(Dec::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(Error::OverflowError("%".to_string())),
            },
            F64(rhs) => match Dec::from_f64_retain(rhs) {
                Some(x) => match lhs.checked_rem(x) {
                    Some(y) => Ok(Decimal(y)),
                    None => Err(Error::OverflowError("%".to_string())),
                },
                _ => Err(ValueError::FailedToParseDecimal(rhs.to_string()).into()),
            },
            Decimal(rhs) => match lhs.checked_rem(rhs) {
                Some(x) => Ok(Decimal(x)),
                None => Err(Error::OverflowError("%".to_string())),
            },

            Null => Ok(Null),
            _ => Err(ValueError::ModuloOnNonNumeric(Decimal(lhs), rhs.clone()).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{TryBinaryOperator, Value::*},
        crate::data::ValueError,
        rust_decimal::prelude::Decimal as Dec,
        std::cmp::Ordering,
    };

    #[test]
    fn eq() {
        let base = Dec::ONE;

        assert_eq!(base, I8(1));
        assert_eq!(base, I64(1));
        assert_eq!(base, F64(1.0));
        assert_eq!(base, Decimal(Dec::ONE));

        assert_ne!(base, Bool(true));
    }

    #[test]
    fn partial_cmp() {
        let base = Dec::ONE;

        assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F64(1.0)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&Decimal(Dec::ONE)), Some(Ordering::Equal));

        assert_eq!(base.partial_cmp(&Bool(true)), None);
    }

    #[test]
    fn try_add() {
        let dec_epislon: Dec = Dec::from_f64_retain(f64::EPSILON).unwrap();
        let base = Dec::ONE;

        assert!(
            matches!(base.try_add(&I8(1)), Ok(Decimal(x)) if (x - Dec::TWO).abs() < dec_epislon )
        );
        assert!(
            matches!(base.try_add(&I64(1)), Ok(Decimal(x)) if (x - Dec::TWO).abs() < dec_epislon )
        );
        assert!(
            matches!(base.try_add(&F64(1.0)), Ok(Decimal(x)) if (x - Dec::TWO).abs() < dec_epislon )
        );
        assert!(
            matches!(base.try_add(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::TWO).abs() < dec_epislon )
        );

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::AddOnNonNumeric(Decimal(Dec::ONE), Bool(true)).into())
        );
    }

    #[test]
    fn try_subtract() {
        let dec_epislon: Dec = Dec::from_f64_retain(f64::EPSILON).unwrap();
        let base = Dec::ONE;

        assert!(
            matches!(base.try_subtract(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ZERO).abs() < dec_epislon  )
        );
        assert!(
            matches!(base.try_subtract(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ZERO).abs() < dec_epislon )
        );
        assert!(
            matches!(base.try_subtract(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ZERO).abs() < dec_epislon  )
        );
        assert!(
            matches!(base.try_subtract(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ZERO).abs() < dec_epislon )
        );

        assert_eq!(
            base.try_subtract(&Bool(true)),
            Err(ValueError::SubtractOnNonNumeric(Decimal(Dec::ONE), Bool(true)).into())
        );
    }

    #[test]
    fn try_multiply() {
        let dec_epislon: Dec = Dec::from_f64_retain(f64::EPSILON).unwrap();
        let base = Dec::ONE;

        assert!(
            matches!(base.try_multiply(&I8(1)), Ok(Decimal(x)) if (x - Dec::ONE).abs() < dec_epislon  )
        );
        assert!(
            matches!(base.try_multiply(&I64(1)), Ok(Decimal(x)) if (x - Dec::ONE).abs() < dec_epislon )
        );
        assert!(
            matches!(base.try_multiply(&F64(1.0)), Ok(Decimal(x)) if (x - Dec::ONE).abs() < dec_epislon  )
        );
        assert!(
            matches!(base.try_multiply(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ONE).abs() < dec_epislon )
        );

        assert_eq!(
            base.try_multiply(&Bool(true)),
            Err(ValueError::MultiplyOnNonNumeric(Decimal(Dec::ONE), Bool(true)).into())
        );
    }

    #[test]
    fn try_divide() {
        let dec_epislon: Dec = Dec::from_f64_retain(f64::EPSILON).unwrap();
        let base = Dec::ONE;

        assert!(
            matches!(base.try_divide(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ONE).abs() < dec_epislon  )
        );
        assert!(
            matches!(base.try_divide(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ONE).abs() < dec_epislon  )
        );
        assert!(
            matches!(base.try_divide(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ONE).abs() < dec_epislon  )
        );
        assert!(
            matches!(base.try_divide(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ONE).abs() < dec_epislon )
        );

        assert_eq!(
            base.try_divide(&Bool(true)),
            Err(ValueError::DivideOnNonNumeric(Decimal(Dec::ONE), Bool(true)).into())
        );
    }

    #[test]
    fn try_modulo() {
        let dec_epislon: Dec = Dec::from_f64_retain(f64::EPSILON).unwrap();
        let base = Dec::ONE;

        assert!(
            matches!(base.try_modulo(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ZERO).abs() < dec_epislon )
        );
        assert!(
            matches!(base.try_modulo(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ZERO).abs() < dec_epislon )
        );
        assert!(
            matches!(base.try_modulo(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ZERO).abs() < dec_epislon )
        );
        assert!(
            matches!(base.try_modulo(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ZERO).abs() < dec_epislon )
        );

        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::ModuloOnNonNumeric(Decimal(Dec::ONE), Bool(true)).into())
        );
    }
}
