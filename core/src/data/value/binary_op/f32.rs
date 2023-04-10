use {
    super::TryBinaryOperator,
    crate::{
        data::{NumericBinaryOperator, ValueError},
        prelude::Value,
        result::Result,
    },
    rust_decimal::prelude::Decimal,
    std::cmp::Ordering,
    Value::*,
};

impl PartialEq<Value> for f32 {
    fn eq(&self, other: &Value) -> bool {
        let lhs = *self;

        match *other {
            I8(rhs) => (lhs - (rhs as f32)).abs() < f32::EPSILON,
            I16(rhs) => (lhs - (rhs as f32)).abs() < f32::EPSILON,
            I32(rhs) => (lhs - (rhs as f32)).abs() < f32::EPSILON,
            I64(rhs) => (lhs - (rhs as f32)).abs() < f32::EPSILON,
            I128(rhs) => (lhs - (rhs as f32)).abs() < f32::EPSILON,
            U8(rhs) => (lhs - (rhs as f32)).abs() < f32::EPSILON,
            U16(rhs) => (lhs - (rhs as f32)).abs() < f32::EPSILON,
            U32(rhs) => (lhs - (rhs as f32)).abs() < f32::EPSILON,
            U64(rhs) => (lhs - (rhs as f32)).abs() < f32::EPSILON,
            U128(rhs) => (lhs - (rhs as f32)).abs() < f32::EPSILON,
            F32(rhs) => (lhs - rhs).abs() < f32::EPSILON,
            F64(rhs) => (lhs - rhs as f32).abs() < f32::EPSILON,
            Decimal(rhs) => Decimal::from_f32_retain(lhs)
                .map(|x| rhs == x)
                .unwrap_or(false),
            _ => false,
        }
    }
}

impl PartialOrd<Value> for f32 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match *other {
            I8(rhs) => self.partial_cmp(&(rhs as f32)),
            I16(rhs) => self.partial_cmp(&(rhs as f32)),
            I32(rhs) => self.partial_cmp(&(rhs as f32)),
            I64(rhs) => self.partial_cmp(&(rhs as f32)),
            I128(rhs) => self.partial_cmp(&(rhs as f32)),
            U8(rhs) => self.partial_cmp(&(rhs as f32)),
            U16(rhs) => self.partial_cmp(&(rhs as f32)),
            U32(rhs) => self.partial_cmp(&(rhs as f32)),
            U64(rhs) => self.partial_cmp(&(rhs as f32)),
            U128(rhs) => self.partial_cmp(&(rhs as f32)),
            F64(rhs) => self.partial_cmp(&(rhs as f32)),
            F32(rhs) => self.partial_cmp(&rhs),
            Decimal(rhs) => Decimal::from_f32_retain(*self)
                .map(|x| x.partial_cmp(&rhs))
                .unwrap_or(None),
            _ => None,
        }
    }
}

impl TryBinaryOperator for f32 {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(F32(lhs + rhs as f32)),
            I16(rhs) => Ok(F32(lhs + rhs as f32)),
            I32(rhs) => Ok(F32(lhs + rhs as f32)),
            I64(rhs) => Ok(F32(lhs + rhs as f32)),
            I128(rhs) => Ok(F32(lhs + rhs as f32)),
            U8(rhs) => Ok(F32(lhs + rhs as f32)),
            U16(rhs) => Ok(F32(lhs + rhs as f32)),
            U32(rhs) => Ok(F32(lhs + rhs as f32)),
            U64(rhs) => Ok(F32(lhs + rhs as f32)),
            U128(rhs) => Ok(F32(lhs + rhs as f32)),
            F64(rhs) => Ok(F32(lhs + rhs as f32)),
            F32(rhs) => Ok(F32(lhs + rhs)),
            Decimal(rhs) => Decimal::from_f32_retain(lhs)
                .map(|x| Ok(Decimal(x + rhs)))
                .unwrap_or_else(|| {
                    Err(ValueError::FloatToDecimalConversionFailure(lhs.into()).into())
                }),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: F32(lhs),
                operator: NumericBinaryOperator::Add,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(F32(lhs - rhs as f32)),
            I16(rhs) => Ok(F32(lhs - rhs as f32)),
            I32(rhs) => Ok(F32(lhs - rhs as f32)),
            I64(rhs) => Ok(F32(lhs - rhs as f32)),
            I128(rhs) => Ok(F32(lhs - rhs as f32)),
            U8(rhs) => Ok(F32(lhs - rhs as f32)),
            U16(rhs) => Ok(F32(lhs - rhs as f32)),
            U32(rhs) => Ok(F32(lhs - rhs as f32)),
            U64(rhs) => Ok(F32(lhs - rhs as f32)),
            U128(rhs) => Ok(F32(lhs - rhs as f32)),
            F64(rhs) => Ok(F32(lhs - rhs as f32)),
            F32(rhs) => Ok(F32(lhs - rhs)),
            Decimal(rhs) => Decimal::from_f32_retain(lhs)
                .map(|x| Ok(Decimal(x - rhs)))
                .unwrap_or_else(|| {
                    Err(ValueError::FloatToDecimalConversionFailure(lhs.into()).into())
                }),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: F32(lhs),
                operator: NumericBinaryOperator::Subtract,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(F32(lhs * rhs as f32)),
            I16(rhs) => Ok(F32(lhs * rhs as f32)),
            I32(rhs) => Ok(F32(lhs * rhs as f32)),
            I64(rhs) => Ok(F32(lhs * rhs as f32)),
            I128(rhs) => Ok(F32(lhs * rhs as f32)),
            U8(rhs) => Ok(F32(lhs * rhs as f32)),
            U16(rhs) => Ok(F32(lhs * rhs as f32)),
            U32(rhs) => Ok(F32(lhs * rhs as f32)),
            U64(rhs) => Ok(F32(lhs * rhs as f32)),
            U128(rhs) => Ok(F32(lhs * rhs as f32)),
            F64(rhs) => Ok(F32(lhs * rhs as f32)),
            F32(rhs) => Ok(F32(lhs * rhs)),
            Interval(rhs) => Ok(Interval(lhs * rhs)),
            Decimal(rhs) => Decimal::from_f32_retain(lhs)
                .map(|x| Ok(Decimal(x * rhs)))
                .unwrap_or_else(|| {
                    Err(ValueError::FloatToDecimalConversionFailure(lhs.into()).into())
                }),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: F32(lhs),
                operator: NumericBinaryOperator::Multiply,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(F32(lhs / rhs as f32)),
            I16(rhs) => Ok(F32(lhs / rhs as f32)),
            I32(rhs) => Ok(F32(lhs / rhs as f32)),
            I64(rhs) => Ok(F32(lhs / rhs as f32)),
            I128(rhs) => Ok(F32(lhs / rhs as f32)),
            U8(rhs) => Ok(F32(lhs / rhs as f32)),
            U16(rhs) => Ok(F32(lhs / rhs as f32)),
            U32(rhs) => Ok(F32(lhs / rhs as f32)),
            U64(rhs) => Ok(F32(lhs / rhs as f32)),
            U128(rhs) => Ok(F32(lhs / rhs as f32)),
            F64(rhs) => Ok(F32(lhs / rhs as f32)),
            F32(rhs) => Ok(F32(lhs / rhs)),
            Decimal(rhs) => Decimal::from_f32_retain(lhs)
                .map(|x| Ok(Decimal(x * rhs)))
                .unwrap_or_else(|| {
                    Err(ValueError::FloatToDecimalConversionFailure(lhs.into()).into())
                }),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: F32(lhs),
                operator: NumericBinaryOperator::Divide,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(F32(lhs % rhs as f32)),
            I16(rhs) => Ok(F32(lhs % rhs as f32)),
            I32(rhs) => Ok(F32(lhs % rhs as f32)),
            I64(rhs) => Ok(F32(lhs % rhs as f32)),
            I128(rhs) => Ok(F32(lhs % rhs as f32)),
            U8(rhs) => Ok(F32(lhs % rhs as f32)),
            U16(rhs) => Ok(F32(lhs % rhs as f32)),
            U32(rhs) => Ok(F32(lhs % rhs as f32)),
            U64(rhs) => Ok(F32(lhs % rhs as f32)),
            U128(rhs) => Ok(F32(lhs % rhs as f32)),
            F64(rhs) => Ok(F32(lhs % rhs as f32)),
            F32(rhs) => Ok(F32(lhs % rhs)),
            Decimal(rhs) => match Decimal::from_f32_retain(lhs) {
                Some(x) => x
                    .checked_rem(rhs)
                    .map(|y| Ok(Decimal(y)))
                    .unwrap_or_else(|| {
                        Err(ValueError::BinaryOperationOverflow {
                            lhs: F32(lhs),
                            operator: NumericBinaryOperator::Modulo,
                            rhs: Decimal(rhs),
                        }
                        .into())
                    }),
                _ => Err(ValueError::FloatToDecimalConversionFailure(lhs.into()).into()),
            },
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: F32(lhs),
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
        let base = 1.0_f32;

        assert_eq!(base, I8(1));
        assert_eq!(base, I16(1));
        assert_eq!(base, I32(1));
        assert_eq!(base, I64(1));
        assert_eq!(base, I128(1));
        assert_eq!(base, U8(1));
        assert_eq!(base, U16(1));
        assert_eq!(base, U32(1));
        assert_eq!(base, U64(1));
        assert_eq!(base, U128(1));
        assert_eq!(base, F64(1.0));
        assert_eq!(base, F32(1.0_f32));
        assert_eq!(base, Decimal(Decimal::from(1)));

        assert_ne!(base, Bool(true));
    }

    #[test]
    fn partial_cmp() {
        let base = 1.0_f32;

        assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I16(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I32(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I128(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U16(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U32(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U128(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F64(1.0)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F32(1.0_f32)), Some(Ordering::Equal));
        assert_eq!(
            base.partial_cmp(&Decimal(Decimal::ONE)),
            Some(Ordering::Equal)
        );

        assert_eq!(base.partial_cmp(&Bool(true)), None);
    }

    #[test]
    fn try_add() {
        let base = 1.0_f32;

        assert!(matches!(base.try_add(&I8(1)), Ok(F32(x)) if (x - 2.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_add(&I16(1)), Ok(F32(x)) if (x - 2.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_add(&I32(1)), Ok(F32(x)) if (x - 2.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_add(&I64(1)), Ok(F32(x)) if (x - 2.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_add(&I128(1)), Ok(F32(x)) if (x - 2.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_add(&U8(1)), Ok(F32(x)) if (x - 2.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_add(&U16(1)), Ok(F32(x)) if (x - 2.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_add(&U32(1)),Ok(F32(x)) if (x-2.0).abs() < f32::EPSILON));
        assert!(matches!(base.try_add(&U64(1)),Ok(F32(x)) if (x-2.0).abs() < f32::EPSILON));
        assert!(matches!(base.try_add(&U128(1)),Ok(F32(x)) if (x-2.0).abs()<f32::EPSILON));
        assert!(matches!(base.try_add(&F64(1.0)), Ok(F32(x)) if (x - 2.0).abs() < f32::EPSILON ));
        assert!(
            matches!(base.try_add(&F32(1.0_f32)), Ok(F32(x)) if (x - 2.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_add(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::TWO)
        );
        assert_eq!(
            f32::MAX.try_add(&Decimal(Decimal::ONE)),
            Err(ValueError::FloatToDecimalConversionFailure(f32::MAX.into()).into())
        );

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: F32(1.0_f32),
                operator: NumericBinaryOperator::Add,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_subtract() {
        let base = 1.0_f32;

        assert!(matches!(base.try_subtract(&I8(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));
        assert!(
            matches!(base.try_subtract(&I16(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_subtract(&I32(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_subtract(&I64(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_subtract(&I128(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );
        assert!(matches!(base.try_subtract(&U8(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));
        assert!(
            matches!(base.try_subtract(&U16(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_subtract(&U32(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );

        assert!(
            matches!(base.try_subtract(&U64(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_subtract(&U128(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );

        assert!(
            matches!(base.try_subtract(&F64(1.0)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_subtract(&F32(1.0_f32)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_subtract(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );
        assert_eq!(
            f32::MIN.try_subtract(&Decimal(Decimal::ONE)),
            Err(ValueError::FloatToDecimalConversionFailure(f32::MIN.into()).into())
        );

        assert_eq!(
            base.try_subtract(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: F32(1.0_f32),
                operator: NumericBinaryOperator::Subtract,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_multiply() {
        let base = 1.0_f32;

        assert!(matches!(base.try_multiply(&I8(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));
        assert!(
            matches!(base.try_multiply(&I16(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&I32(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&I64(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&I128(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(matches!(base.try_multiply(&U8(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));
        assert!(
            matches!(base.try_multiply(&U16(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&U32(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&U64(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&U128(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&F64(1.0)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&F32(1.0_f32)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ONE)
        );
        assert_eq!(
            f32::MAX.try_multiply(&Decimal(Decimal::TWO)),
            Err(ValueError::FloatToDecimalConversionFailure(f32::MAX.into()).into())
        );

        assert_eq!(
            base.try_multiply(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: F32(1.0_f32),
                operator: NumericBinaryOperator::Multiply,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_divide() {
        let base = 1.0_f32;

        assert!(matches!(base.try_divide(&I8(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_divide(&I16(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_divide(&I32(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_divide(&I64(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_divide(&I128(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_divide(&U8(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_divide(&U16(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_divide(&U32(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_divide(&U64(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_divide(&U128(1)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON ));

        assert!(
            matches!(base.try_divide(&F64(1.0)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_divide(&F32(1.0_f32)), Ok(F32(x)) if (x - 1.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_divide(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ONE)
        );
        assert_eq!(
            f32::MIN.try_divide(&Decimal(Decimal::TWO)),
            Err(ValueError::FloatToDecimalConversionFailure(f32::MIN.into()).into())
        );

        assert_eq!(
            base.try_divide(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: F32(1.0_f32),
                operator: NumericBinaryOperator::Divide,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_modulo() {
        let base = 1.0_f32;

        assert!(matches!(base.try_modulo(&I8(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_modulo(&I16(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_modulo(&I32(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_modulo(&I64(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_modulo(&I128(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_modulo(&U8(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_modulo(&U16(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_modulo(&U32(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_modulo(&U64(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));
        assert!(matches!(base.try_modulo(&U128(1)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON ));

        assert!(
            matches!(base.try_modulo(&F64(1.0)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_modulo(&F32(1.0_f32)), Ok(F32(x)) if (x - 0.0).abs() < f32::EPSILON )
        );
        assert!(
            matches!(base.try_modulo(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );
        assert_eq!(
            f32::MAX.try_modulo(&Decimal(Decimal::TWO)),
            Err(ValueError::FloatToDecimalConversionFailure(f32::MAX.into()).into())
        );
        assert_eq!(
            base.try_modulo(&Decimal(Decimal::ZERO)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: F32(base),
                rhs: Decimal(Decimal::ZERO),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );

        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: F32(1.0_f32),
                operator: NumericBinaryOperator::Modulo,
                rhs: Bool(true)
            }
            .into())
        );
    }
}
