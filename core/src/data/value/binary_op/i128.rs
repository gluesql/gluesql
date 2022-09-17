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

impl PartialEq<Value> for i128 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => self == &(*other as i128),
            I16(other) => self == &(*other as i128),
            I32(other) => self == &(*other as i128),
            I64(other) => self == &(*other as i128),
            I128(other) => self == other,
            U8(other) => self == &(*other as i128),
            U16(other) => self == &(*other as i128),
            F64(other) => ((*self as f64) - other).abs() < f64::EPSILON,
            Decimal(other) => Decimal::from(*self) == *other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for i128 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match other {
            I8(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            I16(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            I32(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            I64(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            I128(other) => PartialOrd::partial_cmp(self, other),
            U8(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
            U16(other) => PartialOrd::partial_cmp(self, &(*other as i128)),
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

            I16(rhs) => lhs
                .checked_add(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I16(rhs),
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
            U8(rhs) => lhs
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
            U16(rhs) => lhs
                .checked_add(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),
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
            I16(rhs) => lhs
                .checked_sub(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I16(rhs),
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
            U8(rhs) => lhs
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
            U16(rhs) => lhs
                .checked_sub(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Subtract,
                    }
                    .into()
                })
                .map(I128),
            F64(rhs) => Ok(F64(lhs as f64 - rhs)),
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
            I16(rhs) => lhs
                .checked_mul(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I16(rhs),
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
            U16(rhs) => lhs
                .checked_mul(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Multiply,
                    }
                    .into()
                })
                .map(I128),
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
            I16(rhs) => lhs
                .checked_div(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I16(rhs),
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
            U16(rhs) => lhs
                .checked_div(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Divide,
                    }
                    .into()
                })
                .map(I128),
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
            I16(rhs) => lhs
                .checked_rem(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: I16(rhs),
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
            U16(rhs) => lhs
                .checked_rem(rhs as i128)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I128(lhs),
                        rhs: U16(rhs),
                        operator: NumericBinaryOperator::Modulo,
                    }
                    .into()
                })
                .map(I128),
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
        assert_eq!(I128(1), I8(1));
        assert_eq!(I128(1), I16(1));
        assert_eq!(I128(1), I32(1));
        assert_eq!(I128(1), I64(1));
        assert_eq!(I128(1), I128(1));
        assert_eq!(I128(1), F64(1.0));
        assert_eq!(I128(1), U8(1));
        assert_eq!(I128(1), U16(1));
        assert_eq!(-1_i128, I128(-1));
        assert_eq!(0_i128, I128(0));
        assert_eq!(1_i128, I128(1));
        assert_eq!(i128::MIN, I128(i128::MIN));
        assert_eq!(i128::MAX, I128(i128::MAX));

        //try_add
        assert_eq!(
            i128::MAX.try_add(&I8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I8(1),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_add(&I16(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I16(1),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_add(&I32(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I32(1),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );

        assert_eq!(
            i128::MAX.try_add(&I64(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I64(1),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );

        assert_eq!(
            i128::MAX.try_add(&I128(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I128(1),
                operator: (NumericBinaryOperator::Add)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_add(&U8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: U8(1),
                operator: NumericBinaryOperator::Add
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_add(&U16(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: U16(1),
                operator: NumericBinaryOperator::Add
            }
            .into())
        );

        //try_subtract
        assert_eq!(
            i128::MIN.try_subtract(&I8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MIN),
                rhs: I8(1),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );
        assert_eq!(
            i128::MIN.try_subtract(&I16(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MIN),
                rhs: I16(1),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );
        assert_eq!(
            i128::MIN.try_subtract(&I32(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MIN),
                rhs: I32(1),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );
        assert_eq!(
            i128::MIN.try_subtract(&I64(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MIN),
                rhs: I64(1),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );

        assert_eq!(
            i128::MIN.try_subtract(&I128(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MIN),
                rhs: I128(1),
                operator: (NumericBinaryOperator::Subtract)
            }
            .into())
        );

        assert_eq!(
            i128::MIN.try_subtract(&U8(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MIN),
                rhs: U8(1),
                operator: NumericBinaryOperator::Subtract
            }
            .into())
        );

        assert_eq!(
            i128::MIN.try_subtract(&U16(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MIN),
                rhs: U16(1),
                operator: NumericBinaryOperator::Subtract
            }
            .into())
        );
        //try multiply
        assert_eq!(i128::MAX.try_multiply(&I8(1)), Ok(I128(i128::MAX)));
        assert_eq!(i128::MAX.try_multiply(&I16(1)), Ok(I128(i128::MAX)));
        assert_eq!(i128::MAX.try_multiply(&I32(1)), Ok(I128(i128::MAX)));
        assert_eq!(i128::MAX.try_multiply(&I64(1)), Ok(I128(i128::MAX)));
        assert_eq!(i128::MAX.try_multiply(&I128(1)), Ok(I128(i128::MAX)));
        assert_eq!(i128::MAX.try_multiply(&U8(1)), Ok(I128(i128::MAX)));
        assert_eq!(i128::MAX.try_multiply(&U16(1)), Ok(I128(i128::MAX)));

        assert_eq!(
            i128::MAX.try_multiply(&I8(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I8(2),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_multiply(&I16(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I16(2),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_multiply(&I32(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I32(2),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_multiply(&I64(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I64(2),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_multiply(&I128(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I128(2),
                operator: (NumericBinaryOperator::Multiply)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_multiply(&U8(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: U8(2),
                operator: NumericBinaryOperator::Multiply
            }
            .into())
        );

        assert_eq!(
            i128::MAX.try_multiply(&U16(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: U16(2),
                operator: NumericBinaryOperator::Multiply
            }
            .into())
        );
        //try_divide
        assert_eq!(
            i128::MAX.try_divide(&I8(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I8(0),
                operator: (NumericBinaryOperator::Divide)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_divide(&I16(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I16(0),
                operator: (NumericBinaryOperator::Divide)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_divide(&I32(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I32(0),
                operator: (NumericBinaryOperator::Divide)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_divide(&I64(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I64(0),
                operator: (NumericBinaryOperator::Divide)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_divide(&I128(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I128(0),
                operator: (NumericBinaryOperator::Divide)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_divide(&U8(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: U8(0),
                operator: NumericBinaryOperator::Divide
            }
            .into())
        );

        assert_eq!(
            i128::MAX.try_divide(&U16(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: U16(0),
                operator: NumericBinaryOperator::Divide
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_modulo(&I8(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I8(0),
                operator: (NumericBinaryOperator::Modulo)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_modulo(&I16(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I8(0),
                operator: (NumericBinaryOperator::Modulo)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_modulo(&I32(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I32(0),
                operator: (NumericBinaryOperator::Modulo)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_modulo(&I64(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I64(0),
                operator: (NumericBinaryOperator::Modulo)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_modulo(&I128(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: I128(0),
                operator: (NumericBinaryOperator::Modulo)
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_modulo(&U8(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: U8(0),
                operator: NumericBinaryOperator::Modulo
            }
            .into())
        );
        assert_eq!(
            i128::MAX.try_modulo(&U16(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(i128::MAX),
                rhs: U16(0),
                operator: NumericBinaryOperator::Modulo
            }
            .into())
        );
    }

    #[test]
    fn eq() {
        let base = 1_i128;

        assert_eq!(base, I8(1));
        assert_eq!(base, I16(1));
        assert_eq!(base, I32(1));
        assert_eq!(base, I64(1));
        assert_eq!(base, I128(1));
        assert_eq!(base, U8(1));
        assert_eq!(base, U16(1));
        assert_eq!(base, F64(1.0));
        assert_eq!(base, Decimal(Decimal::ONE));

        assert_ne!(base, Bool(true));
    }

    #[test]
    fn partial_cmp() {
        let base = 1_i128;

        assert_eq!(base.partial_cmp(&I8(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I16(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I32(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I64(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I128(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&U8(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&U16(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&F64(0.0)), Some(Ordering::Greater));

        assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I16(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I32(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I128(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U16(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F64(1.0)), Some(Ordering::Equal));

        assert_eq!(base.partial_cmp(&I8(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I16(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I32(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I64(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I128(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&U8(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&U16(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&F64(2.0)), Some(Ordering::Less));

        assert_eq!(
            base.partial_cmp(&Decimal(Decimal::ONE)),
            Some(Ordering::Equal)
        );

        assert_eq!(base.partial_cmp(&Bool(true)), None);
    }

    #[test]
    fn try_add() {
        let base = 1_i128;

        assert_eq!(base.try_add(&I8(1)), Ok(I128(2)));
        assert_eq!(base.try_add(&I16(1)), Ok(I128(2)));
        assert_eq!(base.try_add(&I32(1)), Ok(I128(2)));
        assert_eq!(base.try_add(&I64(1)), Ok(I128(2)));
        assert_eq!(base.try_add(&I128(1)), Ok(I128(2)));
        assert_eq!(base.try_add(&U8(1)), Ok(I128(2)));
        assert_eq!(base.try_add(&U16(1)), Ok(I128(2)));

        assert!(matches!(base.try_add(&F64(1.0)), Ok(F64(x)) if (x - 2.0).abs() < f64::EPSILON));
        assert_eq!(
            base.try_add(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::TWO))
        );

        assert_eq!(
            base.try_add(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I128(base),
                operator: NumericBinaryOperator::Add,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_subtract() {
        let base = 1_i128;

        assert_eq!(base.try_subtract(&I8(1)), Ok(I128(0)));
        assert_eq!(base.try_subtract(&I16(1)), Ok(I128(0)));
        assert_eq!(base.try_subtract(&I32(1)), Ok(I128(0)));
        assert_eq!(base.try_subtract(&I64(1)), Ok(I128(0)));
        assert_eq!(base.try_subtract(&I128(1)), Ok(I128(0)));
        assert_eq!(base.try_subtract(&U8(1)), Ok(I128(0)));
        assert_eq!(base.try_subtract(&U16(1)), Ok(I128(0)));

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
                lhs: I128(base),
                operator: NumericBinaryOperator::Subtract,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_multiply() {
        let base = 3_i128;

        assert_eq!(base.try_multiply(&I8(2)), Ok(I128(6)));
        assert_eq!(base.try_multiply(&I16(2)), Ok(I128(6)));
        assert_eq!(base.try_multiply(&I32(2)), Ok(I128(6)));
        assert_eq!(base.try_multiply(&I64(2)), Ok(I128(6)));
        assert_eq!(base.try_multiply(&I128(2)), Ok(I128(6)));

        assert_eq!(base.try_multiply(&I8(-1)), Ok(I128(-3)));
        assert_eq!(base.try_multiply(&I16(-1)), Ok(I128(-3)));
        assert_eq!(base.try_multiply(&I32(-1)), Ok(I128(-3)));
        assert_eq!(base.try_multiply(&I64(-1)), Ok(I128(-3)));
        assert_eq!(base.try_multiply(&I128(-1)), Ok(I128(-3)));

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
                lhs: I128(base),
                operator: NumericBinaryOperator::Multiply,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_divide() {
        let base = 6_i128;

        assert_eq!(base.try_divide(&I8(2)), Ok(I128(3)));
        assert_eq!(base.try_divide(&I16(2)), Ok(I128(3)));
        assert_eq!(base.try_divide(&I32(2)), Ok(I128(3)));
        assert_eq!(base.try_divide(&I64(2)), Ok(I128(3)));
        assert_eq!(base.try_divide(&I128(2)), Ok(I128(3)));
        assert_eq!(base.try_divide(&U8(2)), Ok(I128(3)));
        assert_eq!(base.try_divide(&U16(2)), Ok(I128(3)));

        assert_eq!(base.try_divide(&I8(-6)), Ok(I128(-1)));
        assert_eq!(base.try_divide(&I16(-6)), Ok(I128(-1)));
        assert_eq!(base.try_divide(&I32(-6)), Ok(I128(-1)));
        assert_eq!(base.try_divide(&I64(-6)), Ok(I128(-1)));
        assert_eq!(base.try_divide(&I128(-6)), Ok(I128(-1)));

        assert!(
            matches!(base.try_divide(&F64(1.0)), Ok(F64(x)) if (x - 6.0).abs() < f64::EPSILON )
        );

        assert_eq!(
            base.try_divide(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::from(base)))
        );

        assert_eq!(
            base.try_divide(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I128(base),
                operator: NumericBinaryOperator::Divide,
                rhs: Bool(true)
            }
            .into())
        );
    }

    #[test]
    fn try_modulo() {
        let base = 9_i128;

        assert_eq!(base.try_modulo(&I8(1)), Ok(I128(0)));
        assert_eq!(base.try_modulo(&I16(1)), Ok(I128(0)));
        assert_eq!(base.try_modulo(&I32(1)), Ok(I128(0)));
        assert_eq!(base.try_modulo(&I64(1)), Ok(I128(0)));
        assert_eq!(base.try_modulo(&I128(1)), Ok(I128(0)));
        assert_eq!(base.try_modulo(&U8(1)), Ok(I128(0)));
        assert_eq!(base.try_modulo(&U16(1)), Ok(I128(0)));

        assert_eq!(base.try_modulo(&I8(2)), Ok(I128(1)));
        assert_eq!(base.try_modulo(&I16(2)), Ok(I128(1)));
        assert_eq!(base.try_modulo(&I32(2)), Ok(I128(1)));
        assert_eq!(base.try_modulo(&I64(2)), Ok(I128(1)));
        assert_eq!(base.try_modulo(&I128(2)), Ok(I128(1)));

        assert!(matches!(base.try_modulo(&F64(1.0)), Ok(F64(x)) if (x).abs() < f64::EPSILON ));
        assert_eq!(
            base.try_modulo(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ZERO))
        );

        assert_eq!(
            base.try_modulo(&Bool(true)),
            Err(ValueError::NonNumericMathOperation {
                lhs: I128(base),
                operator: NumericBinaryOperator::Modulo,
                rhs: Bool(true)
            }
            .into())
        );
    }
}
