use {
    super::TryBinaryOperator,
    crate::{
        data::{NumericBinaryOperator, ValueError},
        prelude::Value,
        result::{Error, Result},
    },
    rust_decimal::prelude::Decimal,
    std::cmp::Ordering,
    Value::*,
};

impl PartialEq<Value> for Decimal {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => *self == Decimal::from(*other),
            I64(other) => *self == Decimal::from(*other),
            F64(other) => Decimal::from_f64_retain(*other)
                .map(|x| *self == x)
                .unwrap_or(false),
            Decimal(other) => *self == *other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for Decimal {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match *other {
            I8(rhs) => self.partial_cmp(&(Decimal::from(rhs))),
            I64(rhs) => self.partial_cmp(&(Decimal::from(rhs))),
            F64(rhs) => match Decimal::from_f64_retain(rhs) {
                Some(x) => self.partial_cmp(&x),
                _ => None,
            },
            Decimal(rhs) => self.partial_cmp(&rhs),
            _ => None,
        }
    }
}

impl TryBinaryOperator for Decimal {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(Decimal(lhs + Decimal::from(rhs))),
            I64(rhs) => Ok(Decimal(lhs + Decimal::from(rhs))),
            F64(rhs) => match Decimal::from_f64_retain(rhs) {
                Some(x) => Ok(Decimal(lhs + x)),
                _ => Err(ValueError::F64ToDecimalConversionError(rhs)),
            },
            Decimal(rhs) => Ok(Decimal(lhs + rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(lhs),
                operator: NumericBinaryOperator::Add,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(Decimal(lhs - Decimal::from(rhs))),
            I64(rhs) => Ok(Decimal(lhs - Decimal::from(rhs))),
            F64(rhs) => match Decimal::from_f64_retain(rhs) {
                Some(x) => Ok(Decimal(lhs - x)),
                _ => Err(ValueError::F64ToDecimalConversionError(rhs)),
            },
            Decimal(rhs) => Ok(Decimal(lhs - rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(lhs),
                operator: NumericBinaryOperator::Subtract,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(Decimal(lhs * Decimal::from(rhs))),
            I64(rhs) => Ok(Decimal(lhs * Decimal::from(rhs))),
            F64(rhs) => match Decimal::from_f64_retain(rhs) {
                Some(x) => Ok(Decimal(lhs * x)),
                _ => Err(ValueError::F64ToDecimalConversionError(rhs)),
            },
            Decimal(rhs) => Ok(Decimal(lhs * rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(lhs),
                operator: NumericBinaryOperator::Multiply,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(Decimal(lhs / Decimal::from(rhs))),
            I64(rhs) => Ok(Decimal(lhs / Decimal::from(rhs))),
            F64(rhs) => match Decimal::from_f64_retain(rhs) {
                Some(x) => Ok(Decimal(lhs / x)),
                _ => Err(ValueError::F64ToDecimalConversionError(rhs)),
            },

            Decimal(rhs) => Ok(Decimal(lhs / rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(lhs),
                operator: NumericBinaryOperator::Divide,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => match lhs.checked_rem(Decimal::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(Error::OverflowError("%".to_string())),
            },
            I64(rhs) => match lhs.checked_rem(Decimal::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(Error::OverflowError("%".to_string())),
            },
            F64(rhs) => match Decimal::from_f64_retain(rhs) {
                Some(x) => match lhs.checked_rem(x) {
                    Some(y) => Ok(Decimal(y)),
                    None => Err(Error::OverflowError("%".to_string())),
                },
                _ => Err(ValueError::F64ToDecimalConversionError(rhs)),
            },
            Decimal(rhs) => match lhs.checked_rem(rhs) {
                Some(x) => Ok(Decimal(x)),
                _ => Err(Error::OverflowError("%".to_string())),
            },

            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(lhs),
                operator: NumericBinaryOperator::Modulo,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{TryBinaryOperator, Value::*},
        crate::data::{NumericBinaryOperator, ValueError},
        rust_decimal::prelude::Decimal,
        std::cmp::Ordering,
    };

    #[test]
    fn eq() {
        let base = Decimal::ONE;

        assert_eq!(base, I8(1));
        assert_eq!(base, I64(1));
        assert_eq!(base, F64(1.0));
        assert_eq!(base, Decimal(Decimal::ONE));

        assert_ne!(base, Bool(true));
    }

    #[test]
    fn partial_cmp() {
        let base = Decimal::ONE;

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
        let base = Decimal::ONE;

        assert!(matches!(base.try_add(&I8(1)), Ok(Decimal(x)) if x == Decimal::TWO ));
        assert!(matches!(base.try_add(&I64(1)), Ok(Decimal(x)) if x == Decimal::TWO));
        assert!(matches!(base.try_add(&F64(1.0)), Ok(Decimal(x)) if x == Decimal::TWO));
        assert!(
            matches!(base.try_add(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::TWO)
        );

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(base),
                operator: NumericBinaryOperator::Add,
                rhs: Bool(true).clone(),
            }
            .into()),
        );
    }

    #[test]
    fn try_subtract() {
        let base = Decimal::ONE;

        assert!(
            matches!(base.try_subtract(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );
        assert!(
            matches!(base.try_subtract(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );
        assert!(
            matches!(base.try_subtract(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );
        assert!(
            matches!(base.try_subtract(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );

        assert_eq!(
            base.try_subtract(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(base),
                operator: NumericBinaryOperator::Subtract,
                rhs: Bool(true).clone(),
            }
            .into()),
        );
    }

    #[test]
    fn try_multiply() {
        let base = Decimal::ONE;

        assert!(matches!(base.try_multiply(&I8(1)), Ok(Decimal(x)) if x == Decimal::ONE));
        assert!(matches!(base.try_multiply(&I64(1)), Ok(Decimal(x)) if x == Decimal::ONE));
        assert!(matches!(base.try_multiply(&F64(1.0)), Ok(Decimal(x)) if x == Decimal::ONE));
        assert!(
            matches!(base.try_multiply(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ONE)
        );

        assert_eq!(
            base.try_multiply(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(base),
                operator: NumericBinaryOperator::Multiply,
                rhs: Bool(true).clone(),
            }
            .into()),
        );
    }

    #[test]
    fn try_divide() {
        let base = Decimal::ONE;

        assert!(
            matches!(base.try_divide(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ONE)
        );
        assert!(
            matches!(base.try_divide(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ONE)
        );
        assert!(
            matches!(base.try_divide(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ONE)
        );
        assert!(
            matches!(base.try_divide(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ONE)
        );

        assert_eq!(
            base.try_divide(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(base),
                operator: NumericBinaryOperator::Divide,
                rhs: Bool(true).clone(),
            }
            .into()),
        );
    }

    #[test]
    fn try_modulo() {
        let base = Decimal::ONE;

        assert!(
            matches!(base.try_modulo(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );
        assert!(
            matches!(base.try_modulo(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );
        assert!(
            matches!(base.try_modulo(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );
        assert!(
            matches!(base.try_modulo(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );

        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(base),
                operator: NumericBinaryOperator::Modulo,
                rhs: Bool(true).clone(),
            }
            .into()),
        );
    }
}
