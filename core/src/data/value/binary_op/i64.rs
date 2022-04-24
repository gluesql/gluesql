use {
    super::TryBinaryOperator,
    crate::{
        data::ValueError,
        prelude::Value,
        result::{Error, Result},
    },
    rust_decimal::prelude::Decimal,
    std::cmp::Ordering,
    Value::*,
};

impl PartialOrd<Value> for i64 {
    fn partial_cmp(&self, rhs: &Value) -> Option<Ordering> {
        match rhs {
            I8(rhs) => PartialOrd::partial_cmp(self, &(*rhs as i64)),
            I64(rhs) => PartialOrd::partial_cmp(self, rhs),
            F64(rhs) => PartialOrd::partial_cmp(&(*self as f64), rhs),
            Decimal(other) => Decimal::from(*self).partial_cmp(other),
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
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) + rhs)),
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
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) - rhs)),
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
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) * rhs)),
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
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) / rhs)),
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
            Decimal(rhs) => match Decimal::from(lhs).checked_rem(rhs) {
                Some(x) => Ok(Decimal(x)),
                None => Err(Error::OverflowError("%".to_string())),
            },
            Null => Ok(Null),
            _ => Err(ValueError::ModuloOnNonNumeric(I64(lhs), rhs.clone()).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{TryBinaryOperator, Value::*},
        crate::data::ValueError,
        rust_decimal::prelude::Decimal,
        std::cmp::Ordering,
    };

    #[test]
    fn eq() {
        let base = 1_i64;

        assert_eq!(base, I8(1));
        assert_eq!(base, I64(1));
        assert_eq!(base, F64(1.0));
        assert_eq!(base, Decimal(Decimal::ONE));

        assert_ne!(base, Bool(true));
    }

    #[test]
    fn partial_cmp() {
        let base = 1_i64;

        assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F64(1.0)), Some(Ordering::Equal));
        assert_eq!(
            base.partial_cmp(&Decimal(Decimal::ONE)),
            Some(Ordering::Equal)
        );

        assert_eq!(base.partial_cmp(&Bool(true)), None);
    }

    #[test]
    fn try_add() {
        let dec_epislon: Decimal = Decimal::from_f64_retain(f64::EPSILON).unwrap();

        let base = 1_i64;

        assert!(matches!(base.try_add(&I8(1)), Ok(I64(x)) if x == 2 ));
        assert!(matches!(base.try_add(&I64(1)), Ok(I64(x)) if x == 2 ));
        assert!(matches!(base.try_add(&F64(1.0)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON));
        assert!(
            matches!(base.try_add(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if (x - Decimal::TWO).abs() < dec_epislon)
        );

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::AddOnNonNumeric(I64(1), Bool(true)).into())
        );
    }

    #[test]
    fn try_subtract() {
        let dec_epislon: Decimal = Decimal::from_f64_retain(f64::EPSILON).unwrap();
        let base = 1_i64;

        assert!(matches!(base.try_subtract(&I8(1)), Ok(I64(x)) if x == 0 ));
        assert!(matches!(base.try_subtract(&I64(1)), Ok(I64(x)) if x == 0 ));
        assert!(
            matches!(base.try_subtract(&F64(1.0)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON)
        );

        assert!(
            matches!(base.try_subtract(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if (x - Decimal::ZERO).abs() < dec_epislon)
        );

        assert_eq!(
            base.try_subtract(&Bool(true)),
            Err(ValueError::SubtractOnNonNumeric(I64(1), Bool(true)).into())
        );
    }

    #[test]
    fn try_multiply() {
        let dec_epislon: Decimal = Decimal::from_f64_retain(f64::EPSILON).unwrap();

        let base = 1_i64;

        assert!(matches!(base.try_multiply(&I8(1)), Ok(I64(x)) if x == 1 ));
        assert!(matches!(base.try_multiply(&I64(1)), Ok(I64(x)) if x == 1 ));
        assert!(
            matches!(base.try_multiply(&F64(1.0)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if (x - Decimal::ONE).abs() < dec_epislon)
        );

        assert_eq!(
            base.try_multiply(&Bool(true)),
            Err(ValueError::MultiplyOnNonNumeric(I64(1), Bool(true)).into())
        );
    }

    #[test]
    fn try_divide() {
        let dec_epislon: Decimal = Decimal::from_f64_retain(f64::EPSILON).unwrap();
        let base = 1_i64;

        assert!(matches!(base.try_divide(&I8(1)), Ok(I64(x)) if x == 1 ));
        assert!(matches!(base.try_divide(&I64(1)), Ok(I64(x)) if x == 1 ));
        assert!(matches!(base.try_divide(&F64(1.0)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON));
        assert!(
            matches!(base.try_divide(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if (x - Decimal::ONE).abs() < dec_epislon)
        );

        assert_eq!(
            base.try_divide(&Bool(true)),
            Err(ValueError::DivideOnNonNumeric(I64(1), Bool(true)).into())
        );
    }

    #[test]
    fn try_modulo() {
        let dec_epislon: Decimal = Decimal::from_f64_retain(f64::EPSILON).unwrap();
        let base = 1_i64;

        assert!(matches!(base.try_modulo(&I8(1)), Ok(I64(x)) if x == 0 ));
        assert!(matches!(base.try_modulo(&I64(1)), Ok(I64(x)) if x == 0 ));
        assert!(
            matches!(base.try_modulo(&F64(1.0)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_modulo(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if (x - Decimal::ZERO).abs() < dec_epislon )
        );
        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::ModuloOnNonNumeric(I64(1), Bool(true)).into())
        );
    }
}
