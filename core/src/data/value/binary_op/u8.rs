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

impl PartialEq<Value> for u8 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => *self as i16 == *other as i16,
            I32(other) => &(*self as i32) == other,
            I64(other) => &(*self as i64) == other,
            I128(other) => &(*self as i128) == other,
            U8(other) => self == other,
            U32(other) => &(*self as u32) == other,
            U64(other) => &(*self as u64) == other,
            U128(other) => &(*self as u128) == other,
            F64(other) => (*self as f64) == *other,
            Decimal(other) => Decimal::from(*self) == *other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for u8 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match other {
            I8(other) => PartialOrd::partial_cmp(&(*self as i16), &(*other as i16)),
            I32(other) => PartialOrd::partial_cmp(&(*self as i32), other),
            I64(other) => PartialOrd::partial_cmp(&(*self as i64), other),
            I128(other) => PartialOrd::partial_cmp(&(*self as i128), other),
            U8(other) => PartialOrd::partial_cmp(self, other),
            U32(other) => PartialOrd::partial_cmp(&(*self as u32), other),
            U64(other) => PartialOrd::partial_cmp(&(*self as u64), other),
            U128(other) => PartialOrd::partial_cmp(&(*self as u128), other),
            F64(other) => PartialOrd::partial_cmp(&(*self as f64), other),
            Decimal(other) => Decimal::from(*self).partial_cmp(other),
            _ => None,
        }
    }
}

impl TryBinaryOperator for u8 {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => (lhs as i32)
                .checked_add(rhs as i32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I32),
            I32(rhs) => (lhs as i32)
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
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
                        lhs: U8(lhs),
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
                        lhs: U8(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => lhs
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(U8),
            U32(rhs) => (lhs as u32)
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(U32),
            U64(rhs) => (lhs as u64)
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(U64),
            U128(rhs) => (lhs as u128).checked_add(rhs).map(U128).ok_or_else(|| {
                ValueError::BinaryOperationOverflow {
                    lhs: U8(lhs),
                    rhs: U128(rhs),
                    operator: NumericBinaryOperator::Add,
                }
                .into()
            }),
            F64(rhs) => Ok(F64(lhs as f64 + rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) + rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: U8(lhs),
                operator: NumericBinaryOperator::Add,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => (lhs as i32)
                .checked_sub(rhs as i32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I32),
            I32(rhs) => (lhs as i32)
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
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
                        lhs: U8(lhs),
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
                        lhs: U8(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => lhs
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(U8),
            U32(rhs) => (lhs as i64)
                .checked_sub(rhs as i64)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
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
                        lhs: U8(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => (lhs as u128).checked_sub(rhs).map(U128).ok_or_else(|| {
                ValueError::BinaryOperationOverflow {
                    lhs: U8(lhs),
                    rhs: U128(rhs),
                    operator: NumericBinaryOperator::Subtract,
                }
                .into()
            }),

            F64(rhs) => Ok(F64(lhs as f64 - rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) - rhs)),

            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: U8(lhs),
                operator: NumericBinaryOperator::Subtract,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => (lhs as i32)
                .checked_mul(rhs as i32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I32),
            I32(rhs) => (lhs as i32)
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
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
                        lhs: U8(lhs),
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
                        lhs: U8(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => (lhs as u32)
                .checked_mul(rhs as u32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(U32),
            U32(rhs) => (lhs as u32)
                .checked_mul(rhs as u32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(U32),
            U64(rhs) => (lhs as u64)
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(U64),
            U128(rhs) => (lhs as u128).checked_mul(rhs).map(U128).ok_or_else(|| {
                ValueError::BinaryOperationOverflow {
                    lhs: U8(lhs),
                    rhs: U128(rhs),
                    operator: NumericBinaryOperator::Multiply,
                }
                .into()
            }),

            F64(rhs) => Ok(F64(lhs as f64 * rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) * rhs)),
            Interval(rhs) => Ok(Interval(lhs * rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: U8(lhs),
                operator: NumericBinaryOperator::Multiply,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => (lhs as i32)
                .checked_div(rhs as i32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I32),
            I32(rhs) => (lhs as i32)
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
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
                        lhs: U8(lhs),
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
                        lhs: U8(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => lhs
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(U8), // or should we make this a U32?
            U32(rhs) => (lhs as u32)
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(U32),
            U64(rhs) => (lhs as u64)
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(U64),
            U128(rhs) => (lhs as u128).checked_div(rhs).map(U128).ok_or_else(|| {
                ValueError::BinaryOperationOverflow {
                    lhs: U8(lhs),
                    rhs: U128(rhs),
                    operator: NumericBinaryOperator::Divide,
                }
                .into()
            }),

            F64(rhs) => Ok(F64(lhs as f64 / rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) / rhs)),

            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: U8(lhs),
                operator: NumericBinaryOperator::Divide,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }

    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => (lhs as i32)
                .checked_rem(rhs as i32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I32),
            I32(rhs) => (lhs as i32)
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
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
                        lhs: U8(lhs),
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
                        lhs: U8(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),

            U8(rhs) => (lhs)
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(U8),
            U32(rhs) => (lhs as u32)
                .checked_rem(rhs as u32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(U32),
            U64(rhs) => (lhs as u64)
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U8(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(U64),
            U128(rhs) => (lhs as u128).checked_rem(rhs).map(U128).ok_or_else(|| {
                ValueError::BinaryOperationOverflow {
                    lhs: U8(lhs),
                    rhs: U128(rhs),
                    operator: NumericBinaryOperator::Modulo,
                }
                .into()
            }),

            F64(rhs) => Ok(F64(lhs as f64 % rhs)),
            Decimal(rhs) => match Decimal::from(lhs).checked_rem(rhs) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: U8(lhs),
                    operator: NumericBinaryOperator::Modulo,
                    rhs: Decimal(rhs),
                }
                .into()),
            },
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: U8(lhs),
                operator: NumericBinaryOperator::Modulo,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }
}
