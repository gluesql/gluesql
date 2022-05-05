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

impl PartialEq<Value> for i128 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => self == &(*other as i128),
            I32(other) => self == &(*other as i128),
            I64(other) => self == &(*other as i128),
            I128(other) => self == other,
            U8(other) => self == &(*other as i128),
            U32(other) => self == &(*other as i128),
            U64(other) => self == &(*other as i128),
            U128(other) => self == &(*other as i128),
            F64(other) => (*self as f64) == *other, // is this right?
            Decimal(other) => Decimal::from(*self) == *other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for i128 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match other {
            I8(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            I32(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            I64(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            I128(other) => PartialOrd::partial_cmp(self, other),
            U8(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            U32(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            U64(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            U128(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            F64(other) => PartialOrd::partial_cmp(&(*self as f64), other),
            Decimal(other) => Decimal::from(*self).partial_cmp(other),
            _ => None,
        }
    }
}

impl TryBinaryOperator for i128 {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs
                .checked_add(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),
            I32(rhs) => lhs
                .checked_add(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),
            I64(rhs) => lhs
                .checked_add(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),
            I128(rhs) => lhs
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => (lhs as i128)
                .checked_add(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),
            U32(rhs) => (lhs as i128)
                .checked_add(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),
            U64(rhs) => (lhs as i128)
                .checked_add(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_add(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                }),
                Err(_) => Err(ValueError::ConversionErrorFromDataTypeAToDataTypeB {
                    a: DataType::Int128,
                    b: DataType::UInt128,
                    value: U128(rhs),
                }
                .into()),
            },
            F64(rhs) => Ok(F64(lhs as f64 + rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) + rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I128(lhs),
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
                .checked_sub(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),
            I32(rhs) => lhs
                .checked_sub(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),
            I64(rhs) => lhs
                .checked_sub(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),
            I128(rhs) => lhs
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => (lhs as i128)
                .checked_sub(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),
            U32(rhs) => lhs
                .checked_sub(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),
            U64(rhs) => lhs
                .checked_sub(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => lhs.checked_sub(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                }),
                Err(_) => Err(ValueError::ConversionErrorFromDataTypeAToDataTypeB {
                    a: DataType::Int128,
                    b: DataType::Int128,
                    value: U128(rhs),
                }
                .into()),
            },
            F64(rhs) => Ok(F64(lhs as f64 - rhs)), // should this be f128??
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) - rhs)),

            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I128(lhs),
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
                .checked_mul(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),
            I32(rhs) => lhs
                .checked_mul(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),
            I64(rhs) => lhs
                .checked_mul(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),
            I128(rhs) => lhs
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => lhs
                .checked_mul(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),
            U32(rhs) => lhs
                .checked_mul(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),
            U64(rhs) => lhs
                .checked_mul(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => lhs.checked_mul(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                }),
                Err(_) => Err(ValueError::ConversionErrorFromDataTypeAToDataTypeB {
                    a: DataType::Int128,
                    b: DataType::UInt128,
                    value: U128(rhs),
                }
                .into()),
            },
            F64(rhs) => Ok(F64(lhs as f64 * rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) * rhs)),
            Interval(rhs) => Ok(Interval(lhs * rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I128(lhs),
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
                .checked_div(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),
            I32(rhs) => lhs
                .checked_div(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),
            I64(rhs) => lhs
                .checked_div(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),
            I128(rhs) => lhs
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => lhs
                .checked_div(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),
            U32(rhs) => lhs
                .checked_div(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),
            U64(rhs) => lhs
                .checked_div(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_div(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                }),
                Err(_) => Err(ValueError::ConversionErrorFromDataTypeAToDataTypeB {
                    a: DataType::Int128,
                    b: DataType::Int128,
                    value: U128(rhs),
                }
                .into()),
            },
            F64(rhs) => Ok(F64(lhs as f64 / rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) / rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: I128(lhs),
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
                .checked_rem(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),
            I32(rhs) => lhs
                .checked_rem(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),
            I64(rhs) => lhs
                .checked_rem(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),
            I128(rhs) => (lhs as i128)
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => lhs
                .checked_rem(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),
            U32(rhs) => lhs
                .checked_rem(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),
            U64(rhs) => lhs
                .checked_rem(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_rem(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
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
                lhs: I128(lhs),
                operator: NumericBinaryOperator::Modulo,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }
}
