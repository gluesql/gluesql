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

impl PartialEq<Value> for Decimal {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => *self == Decimal::from(*other),
            I32(other) => *self == Decimal::from(*other),
            I64(other) => *self == Decimal::from(*other),
            I128(other) => *self == Decimal::from(*other),
            U8(other) => *self == Decimal::from(*other),
            U16(other) => *self == Decimal::from(*other),
            U32(other) => *self == Decimal::from(*other),
            U64(other) => *self == Decimal::from(*other),
            U128(other) => *self == Decimal::from(*other),
            F32(other) => Decimal::from_f32_retain(*other)
                .map(|x| *self == x)
                .unwrap_or(false),
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
            I32(rhs) => self.partial_cmp(&(Decimal::from(rhs))),
            I64(rhs) => self.partial_cmp(&(Decimal::from(rhs))),
            I128(rhs) => self.partial_cmp(&(Decimal::from(rhs))),
            U8(rhs) => self.partial_cmp(&(Decimal::from(rhs))),
            U16(rhs) => self.partial_cmp(&(Decimal::from(rhs))),
            U32(rhs) => self.partial_cmp(&(Decimal::from(rhs))),
            U64(rhs) => self.partial_cmp(&(Decimal::from(rhs))),
            U128(rhs) => self.partial_cmp(&(Decimal::from(rhs))),
            F32(rhs) => Decimal::from_f32_retain(rhs)
                .map(|x| self.partial_cmp(&x))
                .unwrap_or(None),
            F64(rhs) => Decimal::from_f64_retain(rhs)
                .map(|x| self.partial_cmp(&x))
                .unwrap_or(None),
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
            I8(rhs) => lhs
                .checked_add(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(Decimal),
            I32(rhs) => lhs
                .checked_add(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(Decimal),
            I64(rhs) => lhs
                .checked_add(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(Decimal),
            I128(rhs) => lhs
                .checked_add(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(Decimal),
            U8(rhs) => lhs
                .checked_add(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(Decimal),
            U16(rhs) => lhs
                .checked_add(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(Decimal),
            U32(rhs) => lhs
                .checked_add(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(Decimal),
            U64(rhs) => lhs
                .checked_add(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(Decimal),
            U128(rhs) => lhs
                .checked_add(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(Decimal),

            F32(rhs) => Decimal::from_f32_retain(rhs)
                .map(|x| {
                    lhs.checked_add(x)
                        .ok_or_else(|| {
                            ValueError::BinaryOperationOverflow {
                                lhs: Decimal(lhs),
                                rhs: F32(rhs),
                                operator: NumericBinaryOperator::Add,
                            }
                            .into()
                        })
                        .map(Decimal)
                })
                .unwrap_or_else(|| {
                    Err(ValueError::FloatToDecimalConversionFailure(rhs.into()).into())
                }),
            F64(rhs) => Decimal::from_f64_retain(rhs)
                .map(|x| {
                    lhs.checked_add(x)
                        .ok_or_else(|| {
                            ValueError::BinaryOperationOverflow {
                                lhs: Decimal(lhs),
                                rhs: F64(rhs),
                                operator: NumericBinaryOperator::Add,
                            }
                            .into()
                        })
                        .map(Decimal)
                })
                .unwrap_or_else(|| Err(ValueError::FloatToDecimalConversionFailure(rhs).into())),
            Decimal(rhs) => lhs
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: Decimal(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(Decimal),
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
            I8(rhs) => lhs
                .checked_sub(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(Decimal),
            I32(rhs) => lhs
                .checked_sub(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(Decimal),
            I64(rhs) => lhs
                .checked_sub(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(Decimal),
            I128(rhs) => lhs
                .checked_sub(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(Decimal),
            U8(rhs) => lhs
                .checked_sub(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(Decimal),
            U16(rhs) => lhs
                .checked_sub(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(Decimal),
            U32(rhs) => lhs
                .checked_sub(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(Decimal),
            U64(rhs) => lhs
                .checked_sub(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(Decimal),
            U128(rhs) => lhs
                .checked_sub(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(Decimal),

            F32(rhs) => Decimal::from_f32_retain(rhs)
                .map(|x| {
                    lhs.checked_sub(x)
                        .ok_or_else(|| {
                            ValueError::BinaryOperationOverflow {
                                lhs: Decimal(lhs),
                                rhs: F32(rhs),
                                operator: NumericBinaryOperator::Subtract,
                            }
                            .into()
                        })
                        .map(Decimal)
                })
                .unwrap_or_else(|| {
                    Err(ValueError::FloatToDecimalConversionFailure(rhs.into()).into())
                }),
            F64(rhs) => Decimal::from_f64_retain(rhs)
                .map(|x| {
                    lhs.checked_sub(x)
                        .ok_or_else(|| {
                            ValueError::BinaryOperationOverflow {
                                lhs: Decimal(lhs),
                                rhs: F64(rhs),
                                operator: NumericBinaryOperator::Subtract,
                            }
                            .into()
                        })
                        .map(Decimal)
                })
                .unwrap_or_else(|| Err(ValueError::FloatToDecimalConversionFailure(rhs).into())),
            Decimal(rhs) => lhs
                .checked_sub(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        operator: NumericBinaryOperator::Subtract,
                        rhs: Decimal(rhs),
                    }
                    .into()
                })
                .map(Decimal),
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
            I8(rhs) => lhs
                .checked_mul(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(Decimal),
            I32(rhs) => lhs
                .checked_mul(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(Decimal),
            I64(rhs) => lhs
                .checked_mul(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(Decimal),
            I128(rhs) => lhs
                .checked_mul(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(Decimal),
            U8(rhs) => lhs
                .checked_mul(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(Decimal),
            U16(rhs) => lhs
                .checked_mul(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(Decimal),
            U32(rhs) => lhs
                .checked_mul(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(Decimal),
            U64(rhs) => lhs
                .checked_mul(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(Decimal),
            U128(rhs) => lhs
                .checked_mul(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(Decimal),

            F32(rhs) => Decimal::from_f32_retain(rhs)
                .map(|x| {
                    lhs.checked_mul(x)
                        .ok_or_else(|| {
                            ValueError::BinaryOperationOverflow {
                                lhs: Decimal(lhs),
                                rhs: F32(rhs),
                                operator: NumericBinaryOperator::Multiply,
                            }
                            .into()
                        })
                        .map(Decimal)
                })
                .unwrap_or_else(|| {
                    Err(ValueError::FloatToDecimalConversionFailure(rhs.into()).into())
                }),
            F64(rhs) => Decimal::from_f64_retain(rhs)
                .map(|x| {
                    lhs.checked_mul(x)
                        .ok_or_else(|| {
                            ValueError::BinaryOperationOverflow {
                                lhs: Decimal(lhs),
                                rhs: F64(rhs),
                                operator: NumericBinaryOperator::Multiply,
                            }
                            .into()
                        })
                        .map(Decimal)
                })
                .unwrap_or_else(|| Err(ValueError::FloatToDecimalConversionFailure(rhs).into())),
            Decimal(rhs) => lhs
                .checked_mul(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        operator: NumericBinaryOperator::Multiply,
                        rhs: Decimal(rhs),
                    }
                    .into()
                })
                .map(Decimal),
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
            I8(rhs) => lhs
                .checked_div(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(Decimal),
            I32(rhs) => lhs
                .checked_div(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I32(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(Decimal),
            I64(rhs) => lhs
                .checked_div(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I64(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(Decimal),
            I128(rhs) => lhs
                .checked_div(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: I128(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(Decimal),
            U8(rhs) => lhs
                .checked_div(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(Decimal),
            U16(rhs) => lhs
                .checked_div(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(Decimal),
            U32(rhs) => lhs
                .checked_div(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(Decimal),
            U64(rhs) => lhs
                .checked_div(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(Decimal),
            U128(rhs) => lhs
                .checked_div(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(Decimal),

            F32(rhs) => Decimal::from_f32_retain(rhs)
                .map(|x| {
                    lhs.checked_div(x)
                        .ok_or_else(|| {
                            ValueError::BinaryOperationOverflow {
                                lhs: Decimal(lhs),
                                rhs: F32(rhs),
                                operator: NumericBinaryOperator::Divide,
                            }
                            .into()
                        })
                        .map(Decimal)
                })
                .unwrap_or_else(|| {
                    Err(ValueError::FloatToDecimalConversionFailure(rhs.into()).into())
                }),
            F64(rhs) => Decimal::from_f64_retain(rhs)
                .map(|x| {
                    lhs.checked_div(x)
                        .ok_or_else(|| {
                            ValueError::BinaryOperationOverflow {
                                lhs: Decimal(lhs),
                                rhs: F64(rhs),
                                operator: NumericBinaryOperator::Divide,
                            }
                            .into()
                        })
                        .map(Decimal)
                })
                .unwrap_or_else(|| Err(ValueError::FloatToDecimalConversionFailure(rhs).into())),
            Decimal(rhs) => lhs
                .checked_div(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        operator: NumericBinaryOperator::Divide,
                        rhs: Decimal(rhs),
                    }
                    .into()
                })
                .map(Decimal),
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
            I8(rhs) => lhs
                .checked_rem(Decimal::from(rhs))
                .map(|x| Ok(Decimal(x)))
                .unwrap_or_else(|| {
                    Err(ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        operator: NumericBinaryOperator::Modulo,
                        rhs: I8(rhs),
                    }
                    .into())
                }),
            I32(rhs) => lhs
                .checked_rem(Decimal::from(rhs))
                .map(|x| Ok(Decimal(x)))
                .unwrap_or_else(|| {
                    Err(ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        operator: NumericBinaryOperator::Modulo,
                        rhs: I32(rhs),
                    }
                    .into())
                }),
            I64(rhs) => lhs
                .checked_rem(Decimal::from(rhs))
                .map(|x| Ok(Decimal(x)))
                .unwrap_or_else(|| {
                    Err(ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        operator: NumericBinaryOperator::Modulo,
                        rhs: I64(rhs),
                    }
                    .into())
                }),
            I128(rhs) => lhs
                .checked_rem(Decimal::from(rhs))
                .map(|x| Ok(Decimal(x)))
                .unwrap_or_else(|| {
                    Err(ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        operator: NumericBinaryOperator::Modulo,
                        rhs: I128(rhs),
                    }
                    .into())
                }),
            U8(rhs) => lhs
                .checked_rem(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U8(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(Decimal),
            U16(rhs) => lhs
                .checked_rem(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(Decimal),
            U32(rhs) => lhs
                .checked_rem(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U32(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(Decimal),
            U64(rhs) => lhs
                .checked_rem(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(Decimal),
            U128(rhs) => lhs
                .checked_rem(Decimal::from(rhs))
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        rhs: U128(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(Decimal),

            F32(rhs) => match Decimal::from_f32_retain(rhs) {
                Some(x) => lhs
                    .checked_rem(x)
                    .map(|y| Ok(Decimal(y)))
                    .unwrap_or_else(|| {
                        Err(ValueError::BinaryOperationOverflow {
                            lhs: Decimal(lhs),
                            operator: NumericBinaryOperator::Modulo,
                            rhs: F32(rhs),
                        }
                        .into())
                    }),
                _ => Err(ValueError::FloatToDecimalConversionFailure(rhs.into()).into()),
            },
            F64(rhs) => match Decimal::from_f64_retain(rhs) {
                Some(x) => lhs
                    .checked_rem(x)
                    .map(|y| Ok(Decimal(y)))
                    .unwrap_or_else(|| {
                        Err(ValueError::BinaryOperationOverflow {
                            lhs: Decimal(lhs),
                            operator: NumericBinaryOperator::Modulo,
                            rhs: F64(rhs),
                        }
                        .into())
                    }),
                _ => Err(ValueError::FloatToDecimalConversionFailure(rhs).into()),
            },
            Decimal(rhs) => lhs
                .checked_rem(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        operator: NumericBinaryOperator::Modulo,
                        rhs: Decimal(rhs),
                    }
                    .into()
                })
                .map(Decimal),
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
    fn test_extremes() {
        let base = Decimal::ONE;

        assert_eq!(
            Decimal::MAX.try_add(&Decimal(Decimal::ONE)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: Decimal(Decimal::ONE),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );

        assert_eq!(
            Decimal::MAX.try_add(&I8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: I8(1),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_add(&I32(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: I32(1),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_add(&I64(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: I64(1),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_add(&I128(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: I128(1),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_add(&U8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: U8(1),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );

        assert_eq!(
            Decimal::MAX.try_add(&U16(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: U16(1),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_add(&U32(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: U32(1),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_add(&U64(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: U64(1),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_add(&U128(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: U128(1),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_add(&F32(1.0_f32)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: F32(1.0_f32),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_add(&F64(1.0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: F64(1.0),
                operator: NumericBinaryOperator::Add,
            }
            .into())
        );

        assert_eq!(
            Decimal::MIN.try_subtract(&I8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: I8(1),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );
        assert_eq!(
            Decimal::MIN.try_subtract(&I32(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: I32(1),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );
        assert_eq!(
            Decimal::MIN.try_subtract(&I64(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: I64(1),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );
        assert_eq!(
            Decimal::MIN.try_subtract(&I128(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: I128(1),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );
        assert_eq!(
            Decimal::MIN.try_subtract(&U8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: U8(1),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );
        assert_eq!(
            Decimal::MIN.try_subtract(&U16(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: U16(1),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );
        assert_eq!(
            Decimal::MIN.try_subtract(&U32(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: U32(1),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );
        assert_eq!(
            Decimal::MIN.try_subtract(&U64(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: U64(1),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );
        assert_eq!(
            Decimal::MIN.try_subtract(&U128(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: U128(1),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );
        assert_eq!(
            Decimal::MIN.try_subtract(&F32(1.0_f32)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: F32(1.0_f32),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );
        assert_eq!(
            Decimal::MIN.try_subtract(&F64(1.0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: F64(1.0),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );

        assert_eq!(
            Decimal::MIN.try_subtract(&Decimal(Decimal::ONE)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: Decimal(Decimal::ONE),
                operator: NumericBinaryOperator::Subtract,
            }
            .into())
        );

        assert_eq!(
            Decimal::MAX.try_multiply(&I8(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: I8(2),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_multiply(&I32(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: I32(2),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_multiply(&I64(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: I64(2),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_multiply(&I128(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: I128(2),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_multiply(&U8(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: U8(2),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_multiply(&U16(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: U16(2),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );

        assert_eq!(
            Decimal::MAX.try_multiply(&U32(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: U32(2),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_multiply(&U64(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: U64(2),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_multiply(&U128(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: U128(2),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_multiply(&F32(2.0_f32)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: F32(2.0_f32),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );
        assert_eq!(
            Decimal::MAX.try_multiply(&F64(2.0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: F64(2.0),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );

        assert_eq!(
            Decimal::MAX.try_multiply(&Decimal(Decimal::TWO)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: Decimal(Decimal::TWO),
                operator: NumericBinaryOperator::Multiply,
            }
            .into())
        );

        // try divide overflow
        assert_eq!(
            base.try_divide(&I8(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: I8(0),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );
        assert_eq!(
            base.try_divide(&I32(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: I32(0),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );
        assert_eq!(
            base.try_divide(&I64(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: I64(0),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );

        assert_eq!(
            base.try_divide(&I128(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: I128(0),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );
        assert_eq!(
            base.try_divide(&U8(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: U8(0),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );

        assert_eq!(
            base.try_divide(&U16(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: U16(0),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );
        assert_eq!(
            base.try_divide(&U32(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: U32(0),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );
        assert_eq!(
            base.try_divide(&U64(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: U64(0),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );
        assert_eq!(
            base.try_divide(&U128(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: U128(0),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );
        assert_eq!(
            base.try_divide(&F32(0.0_f32)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: F32(0.0_f32),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );
        assert_eq!(
            base.try_divide(&F64(0.0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: F64(0.0),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );

        assert_eq!(
            base.try_divide(&Decimal(Decimal::ZERO)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: Decimal(Decimal::ZERO),
                operator: NumericBinaryOperator::Divide,
            }
            .into())
        );

        // try modulo overflow
        assert_eq!(
            base.try_modulo(&I8(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: I8(0),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );
        assert_eq!(
            base.try_modulo(&I32(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: I32(0),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );
        assert_eq!(
            base.try_modulo(&I64(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: I64(0),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );

        assert_eq!(
            base.try_modulo(&I128(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: I128(0),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );
        assert_eq!(
            base.try_modulo(&U8(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: U8(0),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );

        assert_eq!(
            base.try_modulo(&U16(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: U16(0),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );
        assert_eq!(
            base.try_modulo(&U32(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: U32(0),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );
        assert_eq!(
            base.try_modulo(&U64(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: U64(0),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );
        assert_eq!(
            base.try_modulo(&U128(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: U128(0),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );
        assert_eq!(
            base.try_modulo(&F32(0.0_f32)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: F32(0.0_f32),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );
        assert_eq!(
            base.try_modulo(&F64(0.0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: F64(0.0),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );

        assert_eq!(
            base.try_modulo(&Decimal(Decimal::ZERO)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: Decimal(Decimal::ZERO),
                operator: NumericBinaryOperator::Modulo,
            }
            .into())
        );
    }

    #[test]
    fn eq() {
        let base = Decimal::ONE;

        assert_eq!(base, I8(1));
        assert_eq!(base, I32(1));
        assert_eq!(base, I64(1));
        assert_eq!(base, I128(1));
        assert_eq!(base, U8(1));
        assert_eq!(base, U16(1));
        assert_eq!(base, U32(1));
        assert_eq!(base, U64(1));
        assert_eq!(base, U128(1));
        assert_eq!(base, F32(1.0_f32));
        assert_eq!(base, F64(1.0));
        assert_eq!(base, Decimal(Decimal::ONE));

        assert_ne!(base, Bool(true));
    }

    #[test]
    fn partial_cmp() {
        let base = Decimal::ONE;

        assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I32(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I128(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U16(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U32(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U128(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F32(1.0_f32)), Some(Ordering::Equal));
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

        assert_eq!(base.try_add(&I8(1)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&I32(1)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&I64(1)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&I128(1)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&U8(1)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&U16(1)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&U32(1)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&U64(1)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&U128(1)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&F32(1.0_f32)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&F64(1.0)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(
            base.try_add(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::TWO))
        );
        assert_eq!(
            base.try_add(&F32(f32::MAX)),
            Err(ValueError::FloatToDecimalConversionFailure(f32::MAX.into()).into())
        );

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(base),
                operator: NumericBinaryOperator::Add,
                rhs: Bool(true),
            }
            .into()),
        );
    }

    #[test]
    fn try_subtract() {
        let base = Decimal::ONE;

        assert_eq!(base.try_subtract(&I8(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_subtract(&I32(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_subtract(&I64(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_subtract(&I128(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_subtract(&U8(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_subtract(&U16(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_subtract(&U32(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_subtract(&U64(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_subtract(&U128(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_subtract(&F32(1.0_f32)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_subtract(&F64(1.0)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(
            base.try_subtract(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ZERO))
        );
        assert_eq!(
            (-base).try_subtract(&F32(f32::MIN)),
            Err(ValueError::FloatToDecimalConversionFailure(f32::MIN.into()).into())
        );

        assert_eq!(
            base.try_subtract(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(base),
                operator: NumericBinaryOperator::Subtract,
                rhs: Bool(true),
            }
            .into()),
        );
    }

    #[test]
    fn try_multiply() {
        let base = Decimal::ONE;

        assert_eq!(base.try_multiply(&I8(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_multiply(&I32(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_multiply(&I64(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_multiply(&I128(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_multiply(&U8(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_multiply(&U16(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_multiply(&U32(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_multiply(&U64(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_multiply(&U128(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_multiply(&F32(1.0_f32)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_multiply(&F64(1.0)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(
            base.try_multiply(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ONE))
        );
        assert_eq!(
            Decimal::TWO.try_multiply(&F32(f32::MAX)),
            Err(ValueError::FloatToDecimalConversionFailure(f32::MAX.into()).into())
        );

        assert_eq!(
            base.try_multiply(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(base),
                operator: NumericBinaryOperator::Multiply,
                rhs: Bool(true),
            }
            .into()),
        );
    }

    #[test]
    fn try_divide() {
        let base = Decimal::ONE;

        assert_eq!(base.try_divide(&I8(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_divide(&I32(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_divide(&I64(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_divide(&I128(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_divide(&U8(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_divide(&U16(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_divide(&U32(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_divide(&U64(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_divide(&U128(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_divide(&F32(1.0_f32)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_divide(&F64(1.0)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(
            base.try_divide(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ONE))
        );
        assert_eq!(
            base.try_divide(&F32(f32::MAX)),
            Err(ValueError::FloatToDecimalConversionFailure(f32::MAX.into()).into())
        );

        assert_eq!(
            base.try_divide(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(base),
                operator: NumericBinaryOperator::Divide,
                rhs: Bool(true),
            }
            .into()),
        );
    }

    #[test]
    fn try_modulo() {
        let base = Decimal::ONE;

        assert_eq!(base.try_modulo(&I8(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_modulo(&I32(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_modulo(&I64(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_modulo(&I128(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_modulo(&U8(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_modulo(&U16(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_modulo(&U32(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_modulo(&U64(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_modulo(&U128(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_modulo(&F32(1.0_f32)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_modulo(&F64(1.0)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(
            base.try_modulo(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ZERO))
        );
        assert_eq!(
            base.try_modulo(&F32(f32::INFINITY)),
            Err(ValueError::FloatToDecimalConversionFailure(f64::INFINITY).into())
        );
        assert_eq!(
            base.try_modulo(&F64(f64::INFINITY)),
            Err(ValueError::FloatToDecimalConversionFailure(f64::INFINITY).into())
        );

        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: Decimal(base),
                operator: NumericBinaryOperator::Modulo,
                rhs: Bool(true),
            }
            .into()),
        );
    }
}
