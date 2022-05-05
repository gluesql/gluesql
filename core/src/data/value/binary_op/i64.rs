use {
    super::TryBinaryOperator,
    crate::{
        ast::DataType,
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
            I32(rhs) => lhs == rhs as i64,
            I64(rhs) => lhs == rhs,
            I128(rhs) => lhs as i128 == rhs,
            U8(rhs) => lhs == rhs as i64,
            U32(rhs) => lhs == rhs as i64,
            U64(rhs) => lhs as i128 == rhs as i128,
            U128(rhs) => lhs as i128 == rhs as i128,

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
            I32(rhs) => PartialOrd::partial_cmp(self, &(*rhs as i64)),
            I64(rhs) => PartialOrd::partial_cmp(self, rhs),
            I128(rhs) => PartialOrd::partial_cmp(&(*self as i128), rhs),
            U8(rhs) => PartialOrd::partial_cmp(self, &(*rhs as i64)),
            U32(rhs) => PartialOrd::partial_cmp(self, &(*rhs as i64)),
            U64(rhs) => PartialOrd::partial_cmp(&(*self as i128), &(*rhs as i128)),
            U128(rhs) => PartialOrd::partial_cmp(&(*self as i128), &(*rhs as i128)),

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
            I32(rhs) => lhs
                .checked_add(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I32(rhs),
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
            I128(rhs) => (lhs as i128)
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => lhs
                .checked_add(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I64),
            U32(rhs) => lhs
                .checked_add(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I64),
            U64(rhs) => (lhs as i128)
                .checked_add(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_add(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                }),
                Err(_) => Err(ValueError::ConversionErrorFromDataTypeAToDataTypeB {
                    a: DataType::UInt128,
                    b: DataType::Int128,
                    value: U128(rhs),
                }
                .into()),
            },
            F64(rhs) => Ok(F64(lhs as f64 + rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) + rhs)),
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
            I32(rhs) => lhs
                .checked_sub(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I32(rhs),
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
            I128(rhs) => (lhs as i128)
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => lhs
                .checked_sub(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I64),
            U32(rhs) => lhs
                .checked_sub(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I64),
            U64(rhs) => (lhs as i128)
                .checked_sub(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_sub(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                }),
                Err(_) => Err(ValueError::ConversionErrorFromDataTypeAToDataTypeB {
                    a: DataType::Int,
                    b: DataType::Int128,
                    value: U128(rhs),
                }
                .into()),
            },
            F64(rhs) => Ok(F64(lhs as f64 - rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) - rhs)),
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
            I32(rhs) => lhs
                .checked_mul(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I32(rhs),
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
            I128(rhs) => (lhs as i128)
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => lhs
                .checked_mul(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I64),
            U32(rhs) => lhs
                .checked_mul(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I64),
            U64(rhs) => (lhs as i128)
                .checked_mul(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_mul(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                }),
                Err(_) => Err(ValueError::ConversionErrorFromDataTypeAToDataTypeB {
                    a: DataType::Int,
                    b: DataType::Int128,
                    value: U128(rhs),
                }
                .into()),
            },
            F64(rhs) => Ok(F64(lhs as f64 * rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) * rhs)),
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
            I32(rhs) => lhs
                .checked_div(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I32(rhs),
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
            I128(rhs) => (lhs as i128)
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => lhs
                .checked_div(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I64),
            U32(rhs) => lhs
                .checked_div(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I64),
            U64(rhs) => (lhs as i128)
                .checked_div(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_div(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                }),
                Err(_) => Err(ValueError::ConversionErrorFromDataTypeAToDataTypeB {
                    a: DataType::Int,
                    b: DataType::Int128,
                    value: U128(rhs),
                }
                .into()),
            },
            F64(rhs) => Ok(F64(lhs as f64 / rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) / rhs)),
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
            I32(rhs) => lhs
                .checked_rem(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I32(rhs),
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
            I128(rhs) => (lhs as i128)
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => lhs
                .checked_rem(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I64),
            U32(rhs) => lhs
                .checked_rem(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I64),
            U64(rhs) => (lhs as i128)
                .checked_rem(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_rem(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I64(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                }),
                Err(_) => Err(ValueError::ConversionErrorFromDataTypeAToDataTypeB {
                    a: DataType::UInt128,
                    b: DataType::Int128,
                    value: U128(rhs),
                }
                .into()),
            },
            F64(rhs) => Ok(F64(lhs as f64 % rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) % rhs)),
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
        let base = 1_i64;

        assert!(matches!(base.try_add(&I8(1)), Ok(I64(x)) if x == 2 ));
        assert!(matches!(base.try_add(&I64(1)), Ok(I64(x)) if x == 2 ));
        assert!(matches!(base.try_add(&F64(1.0)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON));
        assert!(
            matches!(base.try_add(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::TWO)
        );

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I64(1),
                operator: NumericBinaryOperator::Add,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_subtract() {
        let base = 1_i64;

        assert!(matches!(base.try_subtract(&I8(1)), Ok(I64(x)) if x == 0 ));
        assert!(matches!(base.try_subtract(&I64(1)), Ok(I64(x)) if x == 0 ));
        assert!(
            matches!(base.try_subtract(&F64(1.0)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON)
        );

        assert!(
            matches!(base.try_subtract(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );

        assert_eq!(
            base.try_subtract(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I64(1),
                operator: NumericBinaryOperator::Subtract,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_multiply() {
        let base = 1_i64;

        assert!(matches!(base.try_multiply(&I8(1)), Ok(I64(x)) if x == 1 ));
        assert!(matches!(base.try_multiply(&I64(1)), Ok(I64(x)) if x == 1 ));
        assert!(
            matches!(base.try_multiply(&F64(1.0)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ONE)
        );

        assert_eq!(
            base.try_multiply(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I64(1),
                operator: NumericBinaryOperator::Multiply,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_divide() {
        let base = 1_i64;

        assert!(matches!(base.try_divide(&I8(1)), Ok(I64(x)) if x == 1 ));
        assert!(matches!(base.try_divide(&I64(1)), Ok(I64(x)) if x == 1 ));
        assert!(matches!(base.try_divide(&F64(1.0)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON));
        assert!(
            matches!(base.try_divide(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ONE)
        );

        assert_eq!(
            base.try_divide(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I64(1),
                operator: NumericBinaryOperator::Divide,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_modulo() {
        let base = 1_i64;

        assert!(matches!(base.try_modulo(&I8(1)), Ok(I64(x)) if x == 0 ));
        assert!(matches!(base.try_modulo(&I64(1)), Ok(I64(x)) if x == 0 ));
        assert!(
            matches!(base.try_modulo(&F64(1.0)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_modulo(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );
        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I64(1),
                operator: NumericBinaryOperator::Modulo,
                rhs: Bool(true)
            }
            .into())
        );
    }
}
