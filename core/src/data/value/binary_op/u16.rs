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

impl PartialEq<Value> for u16 {
    fn eq(&self, other: &Value) -> bool {
        let lhs = *self;
        match other {
            I8(rhs) => (*rhs as u16) == lhs,
            I16(rhs) => (*rhs as u16) == lhs,
            I32(rhs) => (*rhs as u16) == lhs,
            I64(rhs) => (*rhs as u16) == lhs,
            I128(rhs) => (*rhs as u16) == lhs,
            U8(rhs) => (*rhs as u16) == lhs,
            U16(rhs) => *rhs == lhs,
            F64(rhs) => ((lhs as f64) - rhs).abs() < f64::EPSILON,
            Decimal(rhs) => Decimal::from(lhs) == *rhs,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for u16 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        let lhs = *self;
        match other {
            I8(rhs) => lhs.partial_cmp(&(*rhs as u16)),
            I16(rhs) => lhs.partial_cmp(&(*rhs as u16)),
            I32(rhs) => lhs.partial_cmp(&(*rhs as u16)),
            I64(rhs) => lhs.partial_cmp(&(*rhs as u16)),
            I128(rhs) => lhs.partial_cmp(&(*rhs as u16)),
            U8(rhs) => self.partial_cmp(&(*rhs as u16)),
            U16(rhs) => self.partial_cmp(rhs),
            F64(rhs) => (*self as f64).partial_cmp(rhs),
            Decimal(rhs) => Decimal::from(*self).partial_cmp(rhs),
            _ => None,
        }
    }
}

impl TryBinaryOperator for u16 {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs
                .checked_add(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(U16),
            I16(rhs) => lhs
                .checked_add(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I16(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(U16),
            I32(rhs) => lhs
                .checked_add(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(U16),
            I64(rhs) => lhs
                .checked_add(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(U16),
            I128(rhs) => lhs
                .checked_add(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(U16),
            U8(rhs) => lhs
                .checked_add(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(U16),
            U16(rhs) => Ok(U16(lhs + rhs)),
            F64(rhs) => Ok(F64(lhs as f64 + rhs)),
            Decimal(rhs) => Ok(Decimal(Decimal::from(lhs) + rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: U16(lhs),
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
                .checked_sub(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(U16),
            I16(rhs) => lhs
                .checked_sub(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I16(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(U16),
            I32(rhs) => lhs
                .checked_sub(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(U16),
            I64(rhs) => lhs
                .checked_sub(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(U16),
            I128(rhs) => lhs
                .checked_sub(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(U16),
            U8(rhs) => lhs
                .checked_sub(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(U16),
            U16(rhs) => Ok(U16(lhs - rhs)),
            F64(rhs) => Ok(F64(lhs as f64 - rhs)),
            Decimal(rhs) => Decimal::from(lhs)
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: Decimal(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(Decimal),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: U16(lhs),
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
                .checked_mul(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(U16),
            I16(rhs) => lhs
                .checked_mul(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I16(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(U16),
            I32(rhs) => lhs
                .checked_mul(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(U16),
            I64(rhs) => lhs
                .checked_mul(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(U16),
            I128(rhs) => lhs
                .checked_mul(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(U16),
            U8(rhs) => lhs
                .checked_mul(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(U16),
            U16(rhs) => Ok(U16(lhs * rhs)),
            F64(rhs) => Ok(F64(lhs as f64 * rhs)),
            Decimal(rhs) => Decimal::from(lhs)
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: Decimal(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(Decimal),
            Interval(rhs) => Ok(Interval(lhs * rhs)),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: U16(lhs),
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
                .checked_div(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(U16),
            I16(rhs) => lhs
                .checked_div(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I16(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(U16),
            I32(rhs) => lhs
                .checked_div(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(U16),
            I64(rhs) => lhs
                .checked_div(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(U16),
            I128(rhs) => lhs
                .checked_div(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(U16),
            U8(rhs) => lhs
                .checked_div(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(U16),
            U16(rhs) => lhs
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(U16),
            F64(rhs) => Ok(F64(lhs as f64 / rhs)),
            Decimal(rhs) => Decimal::from(lhs)
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: Decimal(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(Decimal),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: U16(lhs),
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
                .checked_rem(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(U16),
            I16(rhs) => lhs
                .checked_rem(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I16(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(U16),
            I32(rhs) => lhs
                .checked_rem(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(U16),
            I64(rhs) => lhs
                .checked_rem(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(U16),
            I128(rhs) => lhs
                .checked_rem(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(U16),
            U8(rhs) => lhs
                .checked_rem(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(U16),
            U16(rhs) => lhs
                .checked_rem(rhs as u16)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(U16),
            F64(rhs) => Ok(F64(lhs as f64 % rhs)),
            Decimal(rhs) => Decimal::from(lhs)
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: U16(lhs),
                        rhs: Decimal(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(Decimal),
            Null => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: U16(lhs),
                operator: NumericBinaryOperator::Modulo,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use {
//         super::{TryBinaryOperator, Value::*},
//         crate::data::{NumericBinaryOperator, ValueError},
//         rust_decimal::prelude::Decimal,
//         std::cmp::Ordering,
//     };

//     #[test]
//     fn eq() {
//         let base = 1_u16;

//         assert_eq!(base, I8(1));
//         assert_eq!(base, I16(1));
//         assert_eq!(base, I32(1));
//         assert_eq!(base, I64(1));
//         assert_eq!(base, I128(1));
//         assert_eq!(base, U8(1));
//         assert_eq!(base, U16(1));
//         assert_eq!(base, F64(1.0));
//         assert_eq!(base, Decimal(Decimal::ONE));
//         assert_ne!(base, Bool(true));
//     }
// }
