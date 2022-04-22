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

impl PartialEq<Value> for i8 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => self == other,
            I64(other) => &(*self as i64) == other,
            F64(other) => &(*self as f64) == other,
            Decimal(other) => Dec::from(*self) == *other,
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
            Decimal(other) => Dec::from(*self).partial_cmp(other),
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
            Decimal(rhs) => Ok(Decimal(Dec::from(lhs) + rhs)),
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
            Decimal(rhs) => Ok(Decimal(Dec::from(lhs) - rhs)),

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
            Decimal(rhs) => Ok(Decimal(Dec::from(lhs) * rhs)),
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
            Decimal(rhs) => Ok(Decimal(Dec::from(lhs) / rhs)),

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
            Decimal(rhs) => match Dec::from(lhs).checked_rem(rhs) {
                Some(x) => Ok(Decimal(x)),
                None => Err(Error::OverflowError("%".to_string())),
            },
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
            Decimal(rhs) => Dec::from(lhs) == rhs,
            _ => false,
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
        let base = 1_i8;

        assert_eq!(base, I8(1));
        assert_eq!(base, I64(1));
        assert_eq!(base, F64(1.0));
        assert_eq!(base, Decimal(Dec::ONE));

        assert_ne!(base, Bool(true));
    }

    #[test]
    fn partial_cmp() {
        let base = 1_i8;

        assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F64(1.0)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&Decimal(Dec::ONE)), Some(Ordering::Equal));

        assert_eq!(base.partial_cmp(&Bool(true)), None);
    }

    #[test]
    fn try_add() {
        let dec_epislon: Dec = Dec::from_f64_retain(f64::EPSILON).unwrap();
        let base = 1_i8;

        assert!(matches!(base.try_add(&I8(1)), Ok(I8(x)) if x == 2 ));
        assert!(matches!(base.try_add(&I64(1)), Ok(I64(x)) if x == 2 ));
        assert!(matches!(base.try_add(&F64(1.0)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON));
        assert!(matches!(base.try_add(&F64(1.0)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON));
        assert!(
            matches!(base.try_add(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::TWO).abs() < dec_epislon)
        );

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::AddOnNonNumeric(I8(1), Bool(true)).into())
        );
    }

    #[test]
    fn try_subtract() {
        let dec_epislon: Dec = Dec::from_f64_retain(f64::EPSILON).unwrap();
        let base = 1_i8;

        assert!(matches!(base.try_subtract(&I8(1)), Ok(I8(x)) if x == 0 ));
        assert!(matches!(base.try_subtract(&I64(1)), Ok(I64(x)) if x == 0 ));
        assert!(
            matches!(base.try_subtract(&F64(1.0)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_subtract(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ZERO).abs() < dec_epislon )
        );

        assert_eq!(
            base.try_subtract(&Bool(true)),
            Err(ValueError::SubtractOnNonNumeric(I8(1), Bool(true)).into())
        );
    }

    #[test]
    fn try_multiply() {
        let dec_epislon: Dec = Dec::from_f64_retain(f64::EPSILON).unwrap();
        let base = 1_i8;

        assert!(matches!(base.try_multiply(&I8(1)), Ok(I8(x)) if x == 1 ));
        assert!(matches!(base.try_multiply(&I64(1)), Ok(I64(x)) if x == 1 ));
        assert!(
            matches!(base.try_multiply(&F64(1.0)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ONE).abs() < dec_epislon )
        );

        assert_eq!(
            base.try_multiply(&Bool(true)),
            Err(ValueError::MultiplyOnNonNumeric(I8(1), Bool(true)).into())
        );
    }

    #[test]
    fn try_divide() {
        let dec_epislon: Dec = Dec::from_f64_retain(f64::EPSILON).unwrap();
        let base = 1_i8;

        assert!(matches!(base.try_divide(&I8(1)), Ok(I8(x)) if x == 1 ));
        assert!(matches!(base.try_divide(&I64(1)), Ok(I64(x)) if x == 1 ));
        assert!(
            matches!(base.try_divide(&F64(1.0)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_divide(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ONE).abs() < dec_epislon )
        );

        assert_eq!(
            base.try_divide(&Bool(true)),
            Err(ValueError::DivideOnNonNumeric(I8(1), Bool(true)).into())
        );
    }

    #[test]
    fn try_modulo() {
        let dec_epislon: Dec = Dec::from_f64_retain(f64::EPSILON).unwrap();
        let base = 1_i8;

        assert!(matches!(base.try_modulo(&I8(1)), Ok(I8(x)) if x == 0 ));
        assert!(matches!(base.try_modulo(&I64(1)), Ok(I64(x)) if x == 0 ));
        assert!(
            matches!(base.try_modulo(&F64(1.0)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_modulo(&Decimal(Dec::ONE)), Ok(Decimal(x)) if (x - Dec::ZERO).abs() < dec_epislon )
        );

        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::ModuloOnNonNumeric(I8(1), Bool(true)).into())
        );
    }
}
