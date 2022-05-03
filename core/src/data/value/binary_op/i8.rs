use {
    super::TryBinaryOperator,
    crate::{
        data::{NumericBinaryOperator, ValueError},
        prelude::{Value, DataType,},
        result::Result,
    },
    rust_decimal::prelude::Decimal,
    std::cmp::Ordering,
    Value::*,
};

impl PartialEq<Value> for i8 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => self == other,
            I32(other) => &(*self as i32) == other,
            I64(other) => &(*self as i64) == other,
            I128(other) => &(*self as i128) == other,
            U8(other) => &(*self as u8) == other,
            U32(other) => &(*self as u32) == other,
            U64(other) => &(*self as u64) == other,
            U128(other) => &(*self as u128) == other,
            F64(other) => &(*self as f64) == other,
            Decimal(other) => Decimal::from(*self) == *other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for i8 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match other {
            I8(other) => self.partial_cmp(other),
            I32(other) => (*self as i32).partial_cmp(other),
            I64(other) => (*self as i64).partial_cmp(other),
            I128(other) => (*self as i128).partial_cmp(other),
            U8(other) => (*self as u8).partial_cmp(other),
            U32(other) => (*self as u32).partial_cmp(other),
            U64(other) => (*self as u64).partial_cmp(other),
            U128(other) => (*self as u128).partial_cmp(other),
            F64(other) => (*self as f64).partial_cmp(other),
            Decimal(other) => Decimal::from(*self).partial_cmp(other),
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
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I8),
            I32(rhs) => (lhs as i32)
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I32),
            I64(rhs) => (lhs as i64)
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
                        lhs: I8(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),
            F64(rhs) => Ok(F64(lhs as f64 + rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) + rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I8(lhs),
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
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I8),
            I32(rhs) => Ok(I32(lhs as i32 - rhs)),
            I64(rhs) => Ok(I64(lhs as i64 - rhs)),
            I128(rhs) => Ok(I128(lhs as i128 - rhs)),
            F64(rhs) => Ok(F64(lhs as f64 - rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) - rhs)),

            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I8(lhs),
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
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I8),
            I32(rhs) => (lhs as i32)
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I32),
            I64(rhs) => (lhs as i64)
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
                        lhs: I8(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => (lhs as i32)
                .checked_mul(rhs as i32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I32),
            U32(rhs) => (lhs as i64)
                .checked_mul(rhs as i64)
                .map(I64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                }),
                //.map(I32),
            U64(rhs) => (lhs as i128)
                .checked_mul(rhs as i128)
                .map(I128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                }),
                //.map(I64),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128)
                           .checked_mul(x)
                           .map(I128)
                           .ok_or_else(|| {
                                ValueError::BinaryOperationOverflow {
                                   lhs: I8(lhs),
                                   rhs: U128(rhs),
                                   operator: NumericBinaryOperator::Multiply,
                                }.into()
                           }),
                Err(_) => Err(ValueError:: ConversionErrorFromDataTypeAToDataTypeB {
                                a:DataType::UInt128,
                                b:DataType::Int128,
                                value:U128(rhs),
                        }.into())
            },

            F64(rhs) => Ok(F64(lhs as f64 * rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) * rhs)),
            Interval(rhs) => Ok(Interval(lhs * rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I8(lhs),
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
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I8),
            I32(rhs) => Ok(I32(lhs as i32 / rhs)),
            I64(rhs) => Ok(I64(lhs as i64 / rhs)),
            I128(rhs) => Ok(I128(lhs as i128 / rhs)),
            F64(rhs) => Ok(F64(lhs as f64 / rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) / rhs)),

            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I8(lhs),
                operator: NumericBinaryOperator::Divide,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => Ok(I8(lhs % rhs)),
            I32(rhs) => Ok(I32(lhs as i32 % rhs)),
            I64(rhs) => Ok(I64(lhs as i64 % rhs)),
            I128(rhs) => Ok(I128(lhs as i128 % rhs)),
            U8(rhs) => Ok(U8(lhs as u8 % rhs)),
            U32(rhs) => Ok(U32(lhs as u32 % rhs)),
            U64(rhs) => Ok(U64(lhs as u64 % rhs)),
            U128(rhs) => Ok(U128(lhs as u128 % rhs)),
            F64(rhs) => Ok(F64(lhs as f64 % rhs)),
            Decimal(rhs) => match Decimal::from(lhs).checked_rem(rhs) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: I8(lhs),
                    operator: NumericBinaryOperator::Modulo,
                    rhs: Decimal(rhs),
                }
                .into()),
            },
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I8(lhs),
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
        let base = 1_i8;

        assert_eq!(base, I8(1));
        assert_eq!(base, I32(1));
        assert_eq!(base, I64(1));
        assert_eq!(base, I128(1));
        assert_eq!(base, U8(1));
        assert_eq!(base, U32(1));
        assert_eq!(base, U64(1));
        assert_eq!(base, U128(1));
        assert_eq!(base, F64(1.0));
        assert_eq!(base, Decimal(Decimal::ONE));

        assert_ne!(base, Bool(true));
    }

    #[test]
    fn partial_cmp() {
        let base = 1_i8;

        assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I32(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I128(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U32(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U128(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F64(1.0)), Some(Ordering::Equal));
        assert_eq!(
            base.partial_cmp(&Decimal(Decimal::ONE)),
            Some(Ordering::Equal)
        );

        assert_eq!(base.partial_cmp(&Bool(true)), None);
    }

    #[test]
    fn try_add() {
        let base = 1_i8;

        assert!(matches!(base.try_add(&I8(1)), Ok(I8(x)) if x == 2 ));
        assert!(matches!(base.try_add(&I32(1)), Ok(I32(x)) if x == 2 ));
        assert!(matches!(base.try_add(&I64(1)), Ok(I64(x)) if x == 2 ));
        assert!(matches!(base.try_add(&I128(1)), Ok(I128(x)) if x == 2 ));
        //assert!(matches!(base.try_add(&U8(1)), Ok(U8(x)) if x == 2 ));
        //assert!(matches!(base.try_add(&U32(1)), Ok(U32(x)) if x == 2 ));
        //assert!(matches!(base.try_add(&U64(1)), Ok(U64(x)) if x == 2 ));
        //assert!(matches!(base.try_add(&U128(1)), Ok(U128(x)) if x == 2 ));
        assert!(matches!(base.try_add(&F64(1.0)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON));
        assert!(matches!(base.try_add(&F64(1.0)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON));
        assert!(
            matches!(base.try_add(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::TWO)
        );

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I8(1),
                operator: NumericBinaryOperator::Add,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_subtract() {
        let base = 1_i8;

        assert!(matches!(base.try_subtract(&I8(1)), Ok(I8(x)) if x == 0 ));
        assert!(matches!(base.try_subtract(&I32(1)), Ok(I32(x)) if x == 0 ));
        assert!(matches!(base.try_subtract(&I64(1)), Ok(I64(x)) if x == 0 ));
        assert!(matches!(base.try_subtract(&I128(1)), Ok(I128(x)) if x == 0 ));
        //assert!(matches!(base.try_subtract(&U8(1)), Ok(U8(x)) if x == 0 ));
        //assert!(matches!(base.try_subtract(&U32(1)), Ok(U32(x)) if x == 0 ));
        //assert!(matches!(base.try_subtract(&U64(1)), Ok(U64(x)) if x == 0 ));
        //assert!(matches!(base.try_subtract(&U128(1)), Ok(U128(x)) if x == 0 ));
        assert!(
            matches!(base.try_subtract(&F64(1.0)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_subtract(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );

        assert_eq!(
            base.try_subtract(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I8(1),
                operator: NumericBinaryOperator::Subtract,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_multiply() {
        let base = 1_i8;

        assert_eq!(base.try_multiply(&I8(1)), Ok(I8(1)));
        assert_eq!(base.try_multiply(&I32(1)), Ok(I32(1)));
        assert_eq!(base.try_multiply(&I64(1)), Ok(I64(1)));
        assert_eq!(base.try_multiply(&I128(1)), Ok(I128(1)));
        //assert!(matches!(base.try_multiply(&I8(1)), Ok(I8(x)) if x == 1 ));
        //assert!(matches!(base.try_multiply(&I32(1)), Ok(I32(x)) if x == 1 ));
        //assert!(matches!(base.try_multiply(&I64(1)), Ok(I64(x)) if x == 1 ));
        //assert!(matches!(base.try_multiply(&I128(1)), Ok(I128(x)) if x == 1 ));
        assert_eq!(base.try_multiply(&U8(1)), Ok(I32(1i32)));
        
        //assert!(matches!(base.try_multiply(&U8(1)), Ok(U8(x)) if x == 1 ));
        //assert!(matches!(base.try_multiply(&U32(1)), Ok(U32(x)) if x == 1 ));
        //assert!(matches!(base.try_multiply(&U64(1)), Ok(U64(x)) if x == 1 ));
        //assert!(matches!(base.try_multiply(&U128(1)), Ok(U128(x)) if x == 1 ));
        assert!(
            matches!(base.try_multiply(&F64(1.0)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_multiply(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ONE)
        );

        assert_eq!(
            base.try_multiply(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I8(1),
                operator: NumericBinaryOperator::Multiply,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_divide() {
        let base = 1_i8;

        assert!(matches!(base.try_divide(&I8(1)), Ok(I8(x)) if x == 1 ));
        assert!(matches!(base.try_divide(&I32(1)), Ok(I32(x)) if x == 1 ));
        assert!(matches!(base.try_divide(&I64(1)), Ok(I64(x)) if x == 1 ));
        assert!(matches!(base.try_divide(&I128(1)), Ok(I128(x)) if x == 1 ));
        //assert_eq!(base.try_divide(&U8(1)), I32(1));
        //assert!(matches!(base.try_divide(&U8(1)), Ok(U8(x)) if x == 1 ));
        //assert!(matches!(base.try_divide(&U32(1)), Ok(U32(x)) if x == 1 ));
        //assert!(matches!(base.try_divide(&U64(1)), Ok(U64(x)) if x == 1 ));
        //assert!(matches!(base.try_divide(&U128(1)), Ok(U128(x)) if x == 1 ));
        assert!(
            matches!(base.try_divide(&F64(1.0)), Ok(F64(x)) if (x - 1.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_divide(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ONE)
        );

        assert_eq!(
            base.try_divide(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I8(1),
                operator: NumericBinaryOperator::Divide,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_modulo() {
        let base = 1_i8;

        assert!(matches!(base.try_modulo(&I8(1)), Ok(I8(x)) if x == 0 ));
        assert!(matches!(base.try_modulo(&I32(1)), Ok(I32(x)) if x == 0 ));
        assert!(matches!(base.try_modulo(&I64(1)), Ok(I64(x)) if x == 0 ));
        assert!(matches!(base.try_modulo(&I128(1)), Ok(I128(x)) if x == 0 ));
        assert!(matches!(base.try_modulo(&U8(1)), Ok(U8(x)) if x == 0 ));
        assert!(matches!(base.try_modulo(&U32(1)), Ok(U32(x)) if x == 0 ));
        assert!(matches!(base.try_modulo(&U64(1)), Ok(U64(x)) if x == 0 ));
        assert!(matches!(base.try_modulo(&U128(1)), Ok(U128(x)) if x == 0 ));
        assert!(
            matches!(base.try_modulo(&F64(1.0)), Ok(F64(x)) if (x - 0.0).abs() < f64::EPSILON )
        );
        assert!(
            matches!(base.try_modulo(&Decimal(Decimal::ONE)), Ok(Decimal(x)) if x == Decimal::ZERO)
        );

        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I8(1),
                operator: NumericBinaryOperator::Modulo,
                rhs: Bool(true)
            }
            .into())
        );
    }
}
