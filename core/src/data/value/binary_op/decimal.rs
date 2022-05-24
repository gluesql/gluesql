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
            I64(other) => *self == Decimal::from(*other),
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
            I64(rhs) => self.partial_cmp(&(Decimal::from(rhs))),
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
            I8(rhs) => match lhs.checked_add(Decimal::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: I8(rhs),
                    operator: NumericBinaryOperator::Add,
                }
                .into()),
            },
            I64(rhs) => match lhs.checked_add(Decimal::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: I64(rhs),
                    operator: NumericBinaryOperator::Add,
                }
                .into()),
            },
            F64(rhs) => Decimal::from_f64_retain(rhs)
                .map(|x| Ok(Decimal(lhs + x)))
                .unwrap_or_else(|| Err(ValueError::FloatToDecimalConversionFailure(rhs).into())),
            Decimal(rhs) => match lhs.checked_add(rhs) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: Decimal(rhs),
                    operator: NumericBinaryOperator::Add,
                }
                .into()),
            },
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
            I8(rhs) => match lhs.checked_sub(Decimal::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: I8(rhs),
                    operator: NumericBinaryOperator::Subtract,
                }
                .into()),
            },
            I64(rhs) => match lhs.checked_sub(Decimal::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: I64(rhs),
                    operator: NumericBinaryOperator::Subtract,
                }
                .into()),
            },

            F64(rhs) => Decimal::from_f64_retain(rhs)
                .map(|x| Ok(Decimal(lhs - x)))
                .unwrap_or_else(|| Err(ValueError::FloatToDecimalConversionFailure(rhs).into())),
            Decimal(rhs) => match lhs.checked_sub(rhs) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: Decimal(rhs),
                    operator: NumericBinaryOperator::Subtract,
                }
                .into()),
            },
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
            I8(rhs) => match lhs.checked_mul(Decimal::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: I8(rhs),
                    operator: NumericBinaryOperator::Multiply,
                }
                .into()),
            },
            I64(rhs) => match lhs.checked_mul(Decimal::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: I64(rhs),
                    operator: NumericBinaryOperator::Multiply,
                }
                .into()),
            },

            F64(rhs) => Decimal::from_f64_retain(rhs)
                .map(|x| Ok(Decimal(lhs * x)))
                .unwrap_or_else(|| Err(ValueError::FloatToDecimalConversionFailure(rhs).into())),
            Decimal(rhs) => match lhs.checked_mul(rhs) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: Decimal(rhs),
                    operator: NumericBinaryOperator::Multiply,
                }
                .into()),
            },
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
            I8(rhs) => match lhs.checked_div(Decimal::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: I8(rhs),
                    operator: NumericBinaryOperator::Divide,
                }
                .into()),
            },
            I64(rhs) => match lhs.checked_div(Decimal::from(rhs)) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: I64(rhs),
                    operator: NumericBinaryOperator::Divide,
                }
                .into()),
            },
            F64(rhs) => Decimal::from_f64_retain(rhs)
                .map(|x| Ok(Decimal(lhs / x)))
                .unwrap_or_else(|| Err(ValueError::FloatToDecimalConversionFailure(rhs).into())),
            Decimal(rhs) => match lhs.checked_div(rhs) {
                Some(x) => Ok(Decimal(x)),
                None => Err(ValueError::BinaryOperationOverflow {
                    lhs: Decimal(lhs),
                    rhs: Decimal(rhs),
                    operator: NumericBinaryOperator::Divide,
                }
                .into()),
            },
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
                .map(|x| Ok(Decimal(x)))
                .unwrap_or_else(|| {
                    Err(ValueError::BinaryOperationOverflow {
                        lhs: Decimal(lhs),
                        operator: NumericBinaryOperator::Modulo,
                        rhs: Decimal(rhs),
                    }
                    .into())
                }),
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
            Decimal::MAX.try_add(&I64(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: I64(1),
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
            Decimal::MIN.try_subtract(&I64(1)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MIN),
                rhs: I64(1),
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
            Decimal::MAX.try_multiply(&I64(2)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(Decimal::MAX),
                rhs: I64(2),
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
            base.try_divide(&I64(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: I64(0),
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
            base.try_modulo(&I64(0)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: Decimal(base),
                rhs: I64(0),
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
        assert_eq!(base, I64(1));
        assert_eq!(base, F64(1.0));
        assert_eq!(base, Decimal(Decimal::ONE));

        assert_ne!(base, Bool(true));
    }

    #[test]
    fn partial_cmp() {
        let base = Decimal::ONE;

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
        let base = Decimal::ONE;

        assert_eq!(base.try_add(&I8(1)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&I64(1)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(base.try_add(&F64(1.0)), Ok(Decimal(Decimal::TWO)));
        assert_eq!(
            base.try_add(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::TWO))
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
        assert_eq!(base.try_subtract(&I64(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_subtract(&F64(1.0)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(
            base.try_subtract(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ZERO))
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
        assert_eq!(base.try_multiply(&I64(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_multiply(&F64(1.0)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(
            base.try_multiply(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ONE))
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
        assert_eq!(base.try_divide(&I64(1)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(base.try_divide(&F64(1.0)), Ok(Decimal(Decimal::ONE)));
        assert_eq!(
            base.try_divide(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ONE))
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
        assert_eq!(base.try_modulo(&I64(1)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(base.try_modulo(&F64(1.0)), Ok(Decimal(Decimal::ZERO)));
        assert_eq!(
            base.try_modulo(&Decimal(Decimal::ONE)),
            Ok(Decimal(Decimal::ZERO))
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
