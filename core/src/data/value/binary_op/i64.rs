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

impl PartialEq<Value> for i64 {
    fn eq(&self, other: &Value) -> bool {
        let lhs = *self;

        match *other {
            I8(rhs) => lhs == rhs as i64,
            I64(rhs) => lhs == rhs,
            F64(rhs) => lhs as f64 == rhs,
            Decimal(rhs) => Decimal::from(lhs) == rhs,
            _ => false,
        }
    }
}

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
            I8(rhs) => lhs
                .checked_add(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I64),
            I64(rhs) => lhs
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I64),
            F64(rhs) => Ok(F64(lhs as f64 + rhs)),
            Decimal(rhs) => Decimal::from(lhs)
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: Decimal(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(Decimal),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I64(lhs),
                operator: NumericBinaryOperator::Add,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs
                .checked_sub(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I64),
            I64(rhs) => lhs
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I64),
            F64(rhs) => Ok(F64(lhs as f64 - rhs)),
            Decimal(rhs) => Decimal::from(lhs)
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: Decimal(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(Decimal),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I64(lhs),
                operator: NumericBinaryOperator::Subtract,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs
                .checked_mul(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I64),
            I64(rhs) => lhs
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I64),
            F64(rhs) => Ok(F64(lhs as f64 * rhs)),
            Decimal(rhs) => Decimal::from(lhs)
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: Decimal(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(Decimal),
            Interval(rhs) => Ok(Interval(lhs * rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I64(lhs),
                operator: NumericBinaryOperator::Multiply,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs
                .checked_div(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I64),
            I64(rhs) => lhs
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I64),
            F64(rhs) => Ok(F64(lhs as f64 / rhs)),
            Decimal(rhs) => Decimal::from(lhs)
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: Decimal(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(Decimal),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I64(lhs),
                operator: NumericBinaryOperator::Divide,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs
                .checked_rem(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I64),
            I64(rhs) => lhs
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I64),
            F64(rhs) => Ok(F64(lhs as f64 % rhs)),
            Decimal(rhs) => Decimal::from(lhs)
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        operator: NumericBinaryOperator::Modulo,
                        rhs: Decimal(rhs),
                    }
                    .into()
                })
                .map(Decimal),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I64(lhs),
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
    fn test_extremes() {
        let type_max: i64 = i64::MAX;
        let type_min: i64 = i64::MIN;
        let type_maxi64: i64 = type_max;

        assert_eq!(-1i64, I64(-1));
        assert_eq!(0i64, I64(0));
        assert_eq!(1i64, I64(1));
        assert_eq!(type_min, I64(type_min));
        assert_eq!(type_max, I64(type_max));

        assert_eq!(
            type_max.try_add(&I8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(type_max),
                rhs: I8(1),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );

        assert_eq!(
            type_max.try_add(&I64(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(type_max),
                rhs: I64(1),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );

        //try_subtract
        assert_eq!(
            type_min.try_subtract(&I8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(type_min),
                rhs: I8(1),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );

        assert_eq!(
            type_min.try_subtract(&I64(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(type_min),
                rhs: I64(1),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );

        //try multiply
        assert_eq!(type_max.try_multiply(&I8(1)), Ok(I64(type_max)));
        assert_eq!(type_max.try_multiply(&I64(1)), Ok(I64(type_maxi64)));

        assert_eq!(
            type_max.try_multiply(&I8(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(type_max),
                rhs: I8(2),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );

        assert_eq!(
            type_max.try_multiply(&I64(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(type_max),
                rhs: I64(2),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );

        //try_divide
        assert_eq!(
            type_max.try_divide(&I8(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(type_max),
                rhs: I8(0),
                operator: (NumericBinaryOperator::Divide)
            }
            .into())
        );

        assert_eq!(
            type_max.try_divide(&I64(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(type_max),
                rhs: I64(0),
                operator: (NumericBinaryOperator::Divide)
            }
            .into())
        );

        assert_eq!(
            type_max.try_modulo(&I8(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(type_max),
                rhs: I8(0),
                operator: (NumericBinaryOperator::Modulo)
            }
            .into())
        );

        assert_eq!(
            type_max.try_modulo(&I64(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(type_max),
                rhs: I64(0),
                operator: (NumericBinaryOperator::Modulo)
            }
            .into())
        );
    }

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

        assert_eq!(base.partial_cmp(&I8(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I64(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&F64(0.0)), Some(Ordering::Greater));

        assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F64(1.0)), Some(Ordering::Equal));

        assert_eq!(base.partial_cmp(&I8(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I64(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&F64(2.0)), Some(Ordering::Less));

        assert_eq!(
            base.partial_cmp(&Decimal(Decimal::ONE)),
            Some(Ordering::Equal)
        );

        assert_eq!(base.partial_cmp(&Bool(true)), None);
    }

    #[test]
    fn try_add() {
        let base = 1_i64;

        assert_eq!(base.try_add(&I8(1)), Ok(I64(2)));
        assert_eq!(base.try_add(&I64(1)), Ok(I64(2)));

        assert!(matches!(base.try_add(&F64(1.0)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON));
        assert_eq!(
            base.try_add(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::TWO))
        );

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I64(base),
                operator: NumericBinaryOperator::Add,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_subtract() {
        let base = 1_i64;

        assert_eq!(base.try_subtract(&I8(1)), Ok(I64(0)));
        assert_eq!(base.try_subtract(&I64(1)), Ok(I64(0)));

        assert!(
            matches!(base.try_subtract(&F64(1.0)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON )
        );

        assert_eq!(
            base.try_subtract(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ZERO))
        );

        assert_eq!(
            base.try_subtract(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I64(base),
                operator: NumericBinaryOperator::Subtract,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_multiply() {
        let base = 3_i64;

        // 3 * 2 = 6
        assert_eq!(base.try_multiply(&I8(2)), Ok(I8(6)));
        assert_eq!(base.try_multiply(&I64(2)), Ok(I64(6)));

        assert_eq!(base.try_multiply(&I8(-1)), Ok(I8(-3)));
        assert_eq!(base.try_multiply(&I64(-1)), Ok(I64(-3)));

        assert!(
            matches!(base.try_multiply(&F64(1.0)), Ok(F64(x)) if (x - 3.0).abs() < f64::EPSILON )
        );

        let _result: Decimal = Decimal::from(3);
        assert_eq!(
            base.try_multiply(&Decimal(Decimal::ONE)),
            Ok(Decimal(_result))
        );

        assert_eq!(
            base.try_multiply(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I64(base),
                operator: NumericBinaryOperator::Multiply,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_divide() {
        let base = 6_i64;

        // 6/2 = 3
        assert_eq!(base.try_divide(&I8(2)), Ok(I64(3)));
        assert_eq!(base.try_divide(&I64(2)), Ok(I64(3)));

        // 6/-6 = -1
        assert_eq!(base.try_divide(&I8(-6)), Ok(I64(-1)));
        assert_eq!(base.try_divide(&I64(-6)), Ok(I64(-1)));

        assert!(
            matches!(base.try_divide(&F64(1.0)), Ok(F64(x)) if (x - 6.0).abs() < f64::EPSILON )
        );

        let _decimal_result = Decimal::from(base);
        assert_eq!(
            base.try_divide(&Decimal(Decimal::ONE)),
            Ok(Decimal(_decimal_result))
        );

        assert_eq!(
            base.try_divide(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I64(base),
                operator: NumericBinaryOperator::Divide,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_modulo() {
        let base = 9_i64;

        assert_eq!(base.try_modulo(&I8(1)), Ok(I64(0)));
        assert_eq!(base.try_modulo(&I64(1)), Ok(I64(0)));

        assert_eq!(base.try_modulo(&I8(2)), Ok(I64(1)));
        assert_eq!(base.try_modulo(&I64(2)), Ok(I64(1)));

        assert!(matches!(base.try_modulo(&F64(1.0)), Ok(F64(x)) if (x).abs() < f64::EPSILON ));
        assert_eq!(
            base.try_modulo(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ZERO))
        );

        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I64(base),
                operator: NumericBinaryOperator::Modulo,
                rhs: Bool(true)
            }
            .into())
        );
    }
}
