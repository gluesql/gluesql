use {
    super::TryBinaryOperator,
    crate::{
        data::{NumericBinaryOperator, ValueError},
        prelude::{DataType, Value},
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
            I32(other) => (*self as i32) == *other,
            I64(other) => (*self as i64) == *other,
            I128(other) => (*self as i128) == *other,
            U8(other) => (*self as i16) == (*other as i16),
            U32(other) => (*self as i64) == (*other as i64),
            U64(other) => (*self as i128) == (*other as i128),
            U128(other) => (*self as i128) == (*other as i128),
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
            U8(other) => (*self as i16).partial_cmp(&(*other as i16)),
            U32(other) => (*self as i64).partial_cmp(&(*other as i64)),
            U64(other) => (*self as i128).partial_cmp(&(*other as i128)),
            U128(other) => (*self as i128).partial_cmp(&(*other as i128)),
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

            U8(rhs) => (lhs as i32)
                .checked_add(rhs as i32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I32),
            U32(rhs) => (lhs as i64)
                .checked_add(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
                        lhs: I8(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_add(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
            I32(rhs) => (lhs as i32)
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I32),
            I64(rhs) => (lhs as i64)
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
                        lhs: I8(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => (lhs as i32)
                .checked_sub(rhs as i32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I32),
            U32(rhs) => (lhs as i64)
                .checked_sub(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
                        lhs: I8(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_sub(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Subtract,
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
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
                        lhs: I8(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_mul(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Multiply,
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
            I32(rhs) => (lhs as i32)
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I32),
            I64(rhs) => (lhs as i64)
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
                        lhs: I8(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => (lhs as i32)
                .checked_div(rhs as i32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I32),
            U32(rhs) => (lhs as i64)
                .checked_div(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
                        lhs: I8(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_div(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Divide,
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
            I8(rhs) => lhs
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I8),
            I32(rhs) => (lhs as i32)
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I32),
            I64(rhs) => (lhs as i64)
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
                        lhs: I8(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => (lhs as i32)
                .checked_rem(rhs as i32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I32),
            U32(rhs) => (lhs as i64)
                .checked_rem(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
                        lhs: I8(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_rem(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I8(lhs),
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
        crate::prelude::DataType,
        bigdecimal::ToPrimitive,
        rust_decimal::prelude::Decimal,
        std::cmp::Ordering,
    };

    #[test]
    fn test_extremes() {
        let type_max: i8 = i8::MAX;
        let type_min: i8 = i8::MIN;
        let type_maxi32: i32 = type_max.into();
        let type_mini32: i32 = type_min.into();
        let type_maxi64: i64 = type_max.into();
        let type_mini64: i64 = type_min.into();
        let type_maxi128: i128 = type_max.into();
        let type_mini128: i128 = type_min.into();

        assert_eq!(-1i8, I8(-1));
        assert_eq!(0i8, I8(0));
        assert_eq!(1i8, I8(1));
        assert_eq!(type_min, I8(type_min));
        assert_eq!(type_max, I8(type_max));

        //try_add
        assert_eq!(
            type_max.try_add(&I8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(type_max),
                rhs: I8(1),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );

        assert_eq!(type_max.try_add(&I32(1)), Ok(I32(type_maxi32 + 1)));
        assert_eq!(type_max.try_add(&I64(1)), Ok(I64(type_maxi64 + 1)));
        assert_eq!(type_max.try_add(&I128(1)), Ok(I128(type_maxi128 + 1)));

        assert_eq!(type_max.try_add(&U8(1)), Ok(I32(type_maxi32 + 1)));
        assert_eq!(type_max.try_add(&U32(1)), Ok(I64(type_maxi64 + 1)));
        assert_eq!(type_max.try_add(&U64(1)), Ok(I128(type_maxi128 + 1)));
        assert_eq!(type_max.try_add(&U128(1)), Ok(I128(type_maxi128 + 1)));

        assert_eq!(
            type_max.try_add(&I32(i32::MAX)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(type_max),
                rhs: I32(i32::MAX),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );

        assert_eq!(
            type_max.try_add(&I64(i64::MAX)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(type_max),
                rhs: I64(i64::MAX),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );

        assert_eq!(
            type_max.try_add(&I128(i128::MAX)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(type_max),
                rhs: I128(i128::MAX),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );

        assert_eq!(
            type_max.try_add(&U128(u128::MAX)),
            Err(ValueError::ConversionErrorFromDataTypeAToDataTypeB {
                a: DataType::UInt128,
                b: DataType::Int128,
                value: U128(u128::MAX),
            }
            .into())
        );

        assert_eq!(
            type_max.try_add(&U128(i128::MAX.to_u128().unwrap())),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(type_max),
                rhs: U128(i128::MAX.to_u128().unwrap()),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );

        //try_subtract
        assert_eq!(
            type_min.try_subtract(&I8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(type_min),
                rhs: I8(1),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );

        // these are fine since they are not i8 (ie, i32, i64, i128)
        assert_eq!(type_min.try_subtract(&I32(1)), Ok(I32(type_mini32 - 1)));
        assert_eq!(type_min.try_subtract(&I64(1)), Ok(I64(type_mini64 - 1)));
        assert_eq!(type_min.try_subtract(&I128(1)), Ok(I128(type_mini128 - 1)));

        assert_eq!(
            type_min.try_subtract(&I32(i32::MAX)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(type_min),
                rhs: I32(i32::MAX),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );

        assert_eq!(
            type_min.try_subtract(&I64(i64::MAX)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(type_min),
                rhs: I64(i64::MAX),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );

        assert_eq!(
            type_min.try_subtract(&I128(i128::MAX)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(type_min),
                rhs: I128(i128::MAX),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );

        //the smallest is a i32 since we have unsigned numbers
        assert_eq!(type_min.try_subtract(&U8(1)), Ok(I32(type_mini32 - 1)));
        assert_eq!(type_min.try_subtract(&U32(1)), Ok(I64(type_mini64 - 1)));
        assert_eq!(type_min.try_subtract(&U64(1)), Ok(I128(type_mini128 - 1)));
        assert_eq!(type_min.try_subtract(&U128(1)), Ok(I128(type_mini128 - 1)));

        assert_eq!(
            type_min.try_subtract(&U128(i128::MAX.to_u128().unwrap())),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(type_min),
                rhs: U128(i128::MAX.to_u128().unwrap()),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );

        //try multiply
        assert_eq!(type_max.try_multiply(&I8(1)), Ok(I8(type_max)));
        assert_eq!(type_max.try_multiply(&I32(1)), Ok(I32(type_maxi32)));
        assert_eq!(type_max.try_multiply(&I64(1)), Ok(I64(type_maxi64)));
        assert_eq!(type_max.try_multiply(&I128(1)), Ok(I128(type_maxi128)));

        assert_eq!(
            i8::MAX.try_multiply(&I8(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(i8::MAX),
                rhs: I8(2),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );

        assert_eq!(
            i8::MAX.try_multiply(&I32(i32::MAX)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(i8::MAX),
                rhs: I32(i32::MAX),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );

        assert_eq!(
            2i8.try_multiply(&I64(i64::MAX)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(2),
                rhs: I64(i64::MAX),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );

        assert_eq!(
            2i8.try_multiply(&I128(i128::MAX)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(2),
                rhs: I128(i128::MAX),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );

        assert_eq!(type_max.try_multiply(&U8(1)), Ok(I32(type_maxi32)));
        assert_eq!(type_max.try_multiply(&U32(1)), Ok(I64(type_maxi64)));
        assert_eq!(type_max.try_multiply(&U64(1)), Ok(I128(type_maxi128)));
        assert_eq!(type_max.try_multiply(&U128(1)), Ok(I128(type_maxi128)));

        assert_eq!(
            type_max.try_multiply(&U128(i128::MAX.to_u128().unwrap())),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(type_max),
                rhs: U128(i128::MAX.to_u128().unwrap()),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );

        assert_eq!(type_max.try_multiply(&U8(2)), Ok(I32(2 * type_maxi32)));
        assert_eq!(type_max.try_multiply(&U32(2)), Ok(I64(2 * type_maxi64)));
        assert_eq!(type_max.try_multiply(&U64(2)), Ok(I128(2 * type_maxi128)));
        assert_eq!(type_max.try_multiply(&U128(2)), Ok(I128(2 * type_maxi128)));

        //try_divide

        //try_modulo
    }

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

        assert_eq!(base.partial_cmp(&I8(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I32(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I64(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I128(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&U8(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&U32(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&U64(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&U128(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&F64(0.0)), Some(Ordering::Greater));

        assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I32(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I128(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U32(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U128(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F64(1.0)), Some(Ordering::Equal));

        assert_eq!(base.partial_cmp(&I8(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I32(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I64(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I128(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&U8(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&U32(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&U64(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&U128(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&F64(2.0)), Some(Ordering::Less));

        assert_eq!(
            base.partial_cmp(&Decimal(Decimal::ONE)),
            Some(Ordering::Equal)
        );

        assert_eq!(base.partial_cmp(&Bool(true)), None);
    }

    #[test]
    fn try_add() {
        let base = 1_i8;

        assert_eq!(base.try_add(&I8(1)), Ok(I8(2)));
        assert_eq!(base.try_add(&I32(1)), Ok(I32(2)));
        assert_eq!(base.try_add(&I64(1)), Ok(I64(2)));
        assert_eq!(base.try_add(&I128(1)), Ok(I128(2)));

        assert_eq!(base.try_add(&U8(1)), Ok(I32(2)));
        assert_eq!(base.try_add(&U32(1)), Ok(I64(2)));
        assert_eq!(base.try_add(&U64(1)), Ok(I128(2)));
        assert_eq!(base.try_add(&U128(1)), Ok(I128(2)));

        assert!(matches!(base.try_add(&F64(1.0)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON));
        assert_eq!(
            base.try_add(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::TWO))
        );

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I8(base),
                operator: NumericBinaryOperator::Add,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_subtract() {
        let base = 1_i8;

        assert_eq!(base.try_subtract(&I8(1)), Ok(I8(0)));
        assert_eq!(base.try_subtract(&I32(1)), Ok(I32(0)));
        assert_eq!(base.try_subtract(&I64(1)), Ok(I64(0)));
        assert_eq!(base.try_subtract(&I128(1)), Ok(I128(0)));

        assert_eq!(base.try_subtract(&U8(1)), Ok(I32(0)));
        assert_eq!(base.try_subtract(&U32(1)), Ok(I64(0)));
        assert_eq!(base.try_subtract(&U64(1)), Ok(I128(0)));
        assert_eq!(base.try_subtract(&U128(1)), Ok(I128(0)));

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
                lhs: I8(base),
                operator: NumericBinaryOperator::Subtract,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_multiply() {
        let base = 3_i8;

        // 3 * 2 = 6
        assert_eq!(base.try_multiply(&I8(2)), Ok(I8(6)));
        assert_eq!(base.try_multiply(&I32(2)), Ok(I32(6)));
        assert_eq!(base.try_multiply(&I64(2)), Ok(I64(6)));
        assert_eq!(base.try_multiply(&I128(2)), Ok(I128(6)));

        assert_eq!(base.try_multiply(&U8(2)), Ok(I32(6)));
        assert_eq!(base.try_multiply(&U32(2)), Ok(I64(6)));
        assert_eq!(base.try_multiply(&U64(2)), Ok(I128(6)));
        assert_eq!(base.try_multiply(&U128(2)), Ok(I128(6)));

        assert_eq!(base.try_multiply(&I8(-1)), Ok(I8(-3)));
        assert_eq!(base.try_multiply(&I32(-1)), Ok(I32(-3)));
        assert_eq!(base.try_multiply(&I64(-1)), Ok(I64(-3)));
        assert_eq!(base.try_multiply(&I128(-1)), Ok(I128(-3)));

        assert_eq!(base.try_multiply(&U8(1)), Ok(I32(3)));
        assert_eq!(base.try_multiply(&U32(1)), Ok(I64(3)));
        assert_eq!(base.try_multiply(&U64(1)), Ok(I128(3)));
        assert_eq!(base.try_multiply(&U128(1)), Ok(I128(3)));

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
                lhs: I8(base),
                operator: NumericBinaryOperator::Multiply,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_divide() {
        let base = 6_i8;

        // 6/2 = 3
        assert_eq!(base.try_divide(&I8(2)), Ok(I8(3)));
        assert_eq!(base.try_divide(&I32(2)), Ok(I32(3)));
        assert_eq!(base.try_divide(&I64(2)), Ok(I64(3)));
        assert_eq!(base.try_divide(&I128(2)), Ok(I128(3)));
        assert_eq!(base.try_divide(&U8(2)), Ok(I32(3)));
        assert_eq!(base.try_divide(&U32(2)), Ok(I64(3)));
        assert_eq!(base.try_divide(&U64(2)), Ok(I128(3)));
        assert_eq!(base.try_divide(&U128(2)), Ok(I128(3)));

        // 6/-6 = -1
        assert_eq!(base.try_divide(&I8(-6)), Ok(I8(-1)));
        assert_eq!(base.try_divide(&I32(-6)), Ok(I32(-1)));
        assert_eq!(base.try_divide(&I64(-6)), Ok(I64(-1)));
        assert_eq!(base.try_divide(&I128(-6)), Ok(I128(-1)));
        // 6/6 = 1  (unsigned numbers in denominator)
        assert_eq!(base.try_divide(&U8(6)), Ok(I32(1)));
        assert_eq!(base.try_divide(&U32(6)), Ok(I64(1)));
        assert_eq!(base.try_divide(&U64(6)), Ok(I128(1)));
        assert_eq!(base.try_divide(&U128(6)), Ok(I128(1)));

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
                lhs: I8(base),
                operator: NumericBinaryOperator::Divide,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_modulo() {
        let base = 9_i8;

        assert_eq!(base.try_modulo(&I8(1)), Ok(I8(0)));
        assert_eq!(base.try_modulo(&I32(1)), Ok(I32(0)));
        assert_eq!(base.try_modulo(&I64(1)), Ok(I64(0)));
        assert_eq!(base.try_modulo(&I128(1)), Ok(I128(0)));

        assert_eq!(base.try_modulo(&U8(1)), Ok(I32(0)));
        assert_eq!(base.try_modulo(&U32(1)), Ok(I64(0)));
        assert_eq!(base.try_modulo(&U64(1)), Ok(I128(0)));
        assert_eq!(base.try_modulo(&U128(1)), Ok(I128(0)));

        assert_eq!(base.try_modulo(&I8(2)), Ok(I8(1)));
        assert_eq!(base.try_modulo(&I32(2)), Ok(I32(1)));
        assert_eq!(base.try_modulo(&I64(2)), Ok(I64(1)));
        assert_eq!(base.try_modulo(&I128(2)), Ok(I128(1)));

        assert_eq!(base.try_modulo(&U8(2)), Ok(I32(1)));
        assert_eq!(base.try_modulo(&U32(2)), Ok(I64(1)));
        assert_eq!(base.try_modulo(&U64(2)), Ok(I128(1)));
        assert_eq!(base.try_modulo(&U128(2)), Ok(I128(1)));
        assert!(matches!(base.try_modulo(&F64(1.0)), Ok(F64(x)) if (x).abs() < f64::EPSILON ));
        assert_eq!(
            base.try_modulo(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ZERO))
        );

        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I8(base),
                operator: NumericBinaryOperator::Modulo,
                rhs: Bool(true)
            }
            .into())
        );
    }
}
