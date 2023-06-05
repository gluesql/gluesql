macro_rules! impl_interval_method {
    (checked_mul, $lhs_variant: ident, $op: ident, $lhs: ident, $rhs: ident) => {
        return Ok(Value::Interval($lhs * $rhs))
    };
    ($other: ident, $lhs_variant: ident, $op: ident, $lhs: ident, $rhs: ident) => {
        return Err(ValueError::NonNumericMathOperation {
            lhs: $lhs_variant($lhs),
            operator: $op,
            rhs: Value::Interval($rhs),
        }
        .into())
    };
}

macro_rules! impl_method {
    ($lhs_variant: ident, $lhs_primitive: ident, $lhs: ident, $method: ident, $op: ident, $rhs: ident) => {{
        match *$rhs {
            I8(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: I8(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            I16(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: I16(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            I32(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: I32(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            I64(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: I64(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            I128(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: I128(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            U8(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: U8(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            U16(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: U16(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            U32(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: U32(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            U64(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: U64(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            U128(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: U128(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            F32(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: F32(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            F64(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: F64(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            Decimal(rhs) => $lhs
                .$method($lhs_primitive::try_from($rhs)?)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: $lhs_variant($lhs),
                        rhs: Decimal(rhs),
                        operator: $op,
                    }
                    .into()
                }),
            Null => return Ok(Null),
            Interval(rhs) => {
                super::macros::impl_interval_method!($method, $lhs_variant, $op, $lhs, rhs);
            }
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: $lhs_variant($lhs),
                operator: $op,
                rhs: $rhs.clone(),
            }
            .into()),
        }
        .map($lhs_variant)
    }};
}

macro_rules! impl_try_binary_op {
    ($variant: ident, $primitive: ident) => {
        use $crate::{
            data::value::{
                error::{NumericBinaryOperator::*, ValueError},
                TryBinaryOperator,
                Value::*,
            },
            result::Result,
        };

        impl TryBinaryOperator for $primitive {
            type Rhs = Value;

            fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
                let lhs = *self;
                super::macros::impl_method!($variant, $primitive, lhs, checked_add, Add, rhs)
            }

            fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
                let lhs = *self;
                super::macros::impl_method!($variant, $primitive, lhs, checked_sub, Subtract, rhs)
            }

            fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
                let lhs = *self;
                super::macros::impl_method!($variant, $primitive, lhs, checked_mul, Multiply, rhs)
            }

            fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
                let lhs = *self;
                super::macros::impl_method!($variant, $primitive, lhs, checked_div, Divide, rhs)
            }

            fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
                let lhs = *self;
                super::macros::impl_method!($variant, $primitive, lhs, checked_rem, Modulo, rhs)
            }
        }
    };
}

#[cfg(test)]
macro_rules! generate_binary_op_tests {
    ($variant: ident, $primitive: ident) => {
        mod try_binary_op_tests {
            use {
                rust_decimal::prelude::Decimal,
                $crate::data::{
                    value::{
                        TryBinaryOperator,
                        Value::{self, *},
                    },
                    NumericBinaryOperator::{self, *},
                    ValueError,
                },
            };

            fn overflow_err(
                lhs: Value,
                rhs: Value,
                op: NumericBinaryOperator,
            ) -> Result<Value, $crate::result::Error> {
                Err(ValueError::BinaryOperationOverflow {
                    lhs,
                    rhs,
                    operator: op,
                }
                .into())
            }

            #[test]
            fn add_overflow() {
                assert_eq!(
                    $primitive::MAX.try_add(&Decimal(Decimal::from(1))),
                    overflow_err($variant($primitive::MAX), Decimal(Decimal::from(1)), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&F32(1.0_f32)),
                    overflow_err($variant($primitive::MAX), F32(1.0_f32), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&F64(1.0)),
                    overflow_err($variant($primitive::MAX), F64(1.0), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&I8(1)),
                    overflow_err($variant($primitive::MAX), I8(1), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&I16(1)),
                    overflow_err($variant($primitive::MAX), I16(1), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&I32(1)),
                    overflow_err($variant($primitive::MAX), I32(1), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&I64(1)),
                    overflow_err($variant($primitive::MAX), I64(1), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&I128(1)),
                    overflow_err($variant($primitive::MAX), I128(1), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&U8(1)),
                    overflow_err($variant($primitive::MAX), U8(1), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&U16(1)),
                    overflow_err($variant($primitive::MAX), U16(1), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&U32(1)),
                    overflow_err($variant($primitive::MAX), U32(1), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&U64(1)),
                    overflow_err($variant($primitive::MAX), U64(1), Add)
                );
                assert_eq!(
                    $primitive::MAX.try_add(&U128(1)),
                    overflow_err($variant($primitive::MAX), U128(1), Add)
                );
            }

            #[test]
            fn sub_overflow() {
                assert_eq!(
                    $primitive::MIN.try_subtract(&Decimal(Decimal::from(1))),
                    overflow_err(
                        $variant($primitive::MIN),
                        Decimal(Decimal::from(1)),
                        Subtract
                    )
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&F32(1.0_f32)),
                    overflow_err($variant($primitive::MIN), F32(1.0_f32), Subtract)
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&F64(1.0)),
                    overflow_err($variant($primitive::MIN), F64(1.0), Subtract)
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&I8(1)),
                    overflow_err($variant($primitive::MIN), I8(1), Subtract)
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&I16(1)),
                    overflow_err($variant($primitive::MIN), I16(1), Subtract)
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&I32(1)),
                    overflow_err($variant($primitive::MIN), I32(1), Subtract)
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&I64(1)),
                    overflow_err($variant($primitive::MIN), I64(1), Subtract)
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&I128(1)),
                    overflow_err($variant($primitive::MIN), I128(1), Subtract)
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&U8(1)),
                    overflow_err($variant($primitive::MIN), U8(1), Subtract)
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&U16(1)),
                    overflow_err($variant($primitive::MIN), U16(1), Subtract)
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&U32(1)),
                    overflow_err($variant($primitive::MIN), U32(1), Subtract)
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&U64(1)),
                    overflow_err($variant($primitive::MIN), U64(1), Subtract)
                );
                assert_eq!(
                    $primitive::MIN.try_subtract(&U128(1)),
                    overflow_err($variant($primitive::MIN), U128(1), Subtract)
                );
            }

            #[test]
            fn mul_overflow() {
                assert_eq!(
                    $primitive::MAX.try_multiply(&Decimal(Decimal::from(2))),
                    overflow_err(
                        $variant($primitive::MAX),
                        Decimal(Decimal::from(2)),
                        Multiply
                    )
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&F32(2.0_f32)),
                    overflow_err($variant($primitive::MAX), F32(2.0_f32), Multiply)
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&F64(2.0)),
                    overflow_err($variant($primitive::MAX), F64(2.0), Multiply)
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&I8(2)),
                    overflow_err($variant($primitive::MAX), I8(2), Multiply)
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&I16(2)),
                    overflow_err($variant($primitive::MAX), I16(2), Multiply)
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&I32(2)),
                    overflow_err($variant($primitive::MAX), I32(2), Multiply)
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&I64(2)),
                    overflow_err($variant($primitive::MAX), I64(2), Multiply)
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&I128(2)),
                    overflow_err($variant($primitive::MAX), I128(2), Multiply)
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&U8(2)),
                    overflow_err($variant($primitive::MAX), U8(2), Multiply)
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&U16(2)),
                    overflow_err($variant($primitive::MAX), U16(2), Multiply)
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&U32(2)),
                    overflow_err($variant($primitive::MAX), U32(2), Multiply)
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&U64(2)),
                    overflow_err($variant($primitive::MAX), U64(2), Multiply)
                );
                assert_eq!(
                    $primitive::MAX.try_multiply(&U128(2)),
                    overflow_err($variant($primitive::MAX), U128(2), Multiply)
                );
            }

            #[test]
            fn div_overflow() {
                assert_eq!(
                    $primitive::MAX.try_divide(&Decimal(Decimal::from(0))),
                    overflow_err($variant($primitive::MAX), Decimal(Decimal::from(0)), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&F32(0.0_f32)),
                    overflow_err($variant($primitive::MAX), F32(0.0_f32), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&F64(0.0)),
                    overflow_err($variant($primitive::MAX), F64(0.0), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&I8(0)),
                    overflow_err($variant($primitive::MAX), I8(0), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&I16(0)),
                    overflow_err($variant($primitive::MAX), I16(0), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&I32(0)),
                    overflow_err($variant($primitive::MAX), I32(0), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&I64(0)),
                    overflow_err($variant($primitive::MAX), I64(0), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&I128(0)),
                    overflow_err($variant($primitive::MAX), I128(0), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&U8(0)),
                    overflow_err($variant($primitive::MAX), U8(0), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&U16(0)),
                    overflow_err($variant($primitive::MAX), U16(0), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&U32(0)),
                    overflow_err($variant($primitive::MAX), U32(0), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&U64(0)),
                    overflow_err($variant($primitive::MAX), U64(0), Divide)
                );
                assert_eq!(
                    $primitive::MAX.try_divide(&U128(0)),
                    overflow_err($variant($primitive::MAX), U128(0), Divide)
                );
            }

            #[test]
            fn mod_overflow() {
                assert_eq!(
                    $primitive::MAX.try_modulo(&Decimal(Decimal::from(0))),
                    overflow_err($variant($primitive::MAX), Decimal(Decimal::from(0)), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&F32(0.0_f32)),
                    overflow_err($variant($primitive::MAX), F32(0.0_f32), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&F64(0.0)),
                    overflow_err($variant($primitive::MAX), F64(0.0), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&I8(0)),
                    overflow_err($variant($primitive::MAX), I8(0), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&I16(0)),
                    overflow_err($variant($primitive::MAX), I16(0), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&I32(0)),
                    overflow_err($variant($primitive::MAX), I32(0), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&I64(0)),
                    overflow_err($variant($primitive::MAX), I64(0), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&I128(0)),
                    overflow_err($variant($primitive::MAX), I128(0), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&U8(0)),
                    overflow_err($variant($primitive::MAX), U8(0), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&U16(0)),
                    overflow_err($variant($primitive::MAX), U16(0), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&U32(0)),
                    overflow_err($variant($primitive::MAX), U32(0), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&U64(0)),
                    overflow_err($variant($primitive::MAX), U64(0), Modulo)
                );
                assert_eq!(
                    $primitive::MAX.try_modulo(&U128(0)),
                    overflow_err($variant($primitive::MAX), U128(0), Modulo)
                );
            }

            #[test]
            fn try_add() {
                let base: $primitive = 1;

                assert_eq!(base.try_add(&Decimal(Decimal::ONE)), Ok($variant(2)));
                assert_eq!(base.try_add(&F32(1.0_f32)), Ok($variant(2)));
                assert_eq!(base.try_add(&F64(1.0)), Ok($variant(2)));
                assert_eq!(base.try_add(&I8(1)), Ok($variant(2)));
                assert_eq!(base.try_add(&I16(1)), Ok($variant(2)));
                assert_eq!(base.try_add(&I32(1)), Ok($variant(2)));
                assert_eq!(base.try_add(&I64(1)), Ok($variant(2)));
                assert_eq!(base.try_add(&I128(1)), Ok($variant(2)));
                assert_eq!(base.try_add(&U8(1)), Ok($variant(2)));
                assert_eq!(base.try_add(&U16(1)), Ok($variant(2)));
                assert_eq!(base.try_add(&U32(1)), Ok($variant(2)));
                assert_eq!(base.try_add(&U64(1)), Ok($variant(2)));
                assert_eq!(base.try_add(&U128(1)), Ok($variant(2)));

                assert_eq!(
                    base.try_add(&Bool(true)),
                    Err(ValueError::NonNumericMathOperation {
                        lhs: $variant(base),
                        operator: NumericBinaryOperator::Add,
                        rhs: Bool(true)
                    }
                    .into())
                );
            }

            #[test]
            fn try_subtract() {
                let base: $primitive = 1;

                assert_eq!(base.try_subtract(&Decimal(Decimal::ONE)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&F32(1.0_f32)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&F64(1.0)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&I8(1)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&I16(1)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&I32(1)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&I64(1)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&I128(1)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&U8(1)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&U16(1)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&U32(1)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&U64(1)), Ok($variant(0)));
                assert_eq!(base.try_subtract(&U128(1)), Ok($variant(0)));

                assert_eq!(
                    base.try_subtract(&Bool(true)),
                    Err(ValueError::NonNumericMathOperation {
                        lhs: $variant(base),
                        operator: NumericBinaryOperator::Subtract,
                        rhs: Bool(true)
                    }
                    .into())
                );
            }

            #[test]
            fn try_multiply() {
                let base: $primitive = 3;

                assert_eq!(base.try_multiply(&Decimal(Decimal::TWO)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&F32(2.0_f32)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&F64(2.0)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&I8(2)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&I16(2)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&I32(2)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&I64(2)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&I128(2)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&U8(2)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&U16(2)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&U32(2)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&U64(2)), Ok($variant(6)));
                assert_eq!(base.try_multiply(&U128(2)), Ok($variant(6)));

                assert_eq!(
                    base.try_multiply(&Bool(true)),
                    Err(ValueError::NonNumericMathOperation {
                        lhs: $variant(base),
                        operator: NumericBinaryOperator::Multiply,
                        rhs: Bool(true)
                    }
                    .into())
                );
            }

            #[test]
            fn try_divide() {
                let base: $primitive = 6;

                assert_eq!(base.try_divide(&Decimal(Decimal::TWO)), Ok($variant(3)));
                assert_eq!(base.try_divide(&F32(2.0_f32)), Ok($variant(3)));
                assert_eq!(base.try_divide(&F64(2.0)), Ok($variant(3)));
                assert_eq!(base.try_divide(&I8(2)), Ok($variant(3)));
                assert_eq!(base.try_divide(&I16(2)), Ok($variant(3)));
                assert_eq!(base.try_divide(&I32(2)), Ok($variant(3)));
                assert_eq!(base.try_divide(&I64(2)), Ok($variant(3)));
                assert_eq!(base.try_divide(&I128(2)), Ok($variant(3)));
                assert_eq!(base.try_divide(&U8(2)), Ok($variant(3)));
                assert_eq!(base.try_divide(&U16(2)), Ok($variant(3)));
                assert_eq!(base.try_divide(&U32(2)), Ok($variant(3)));
                assert_eq!(base.try_divide(&U64(2)), Ok($variant(3)));
                assert_eq!(base.try_divide(&U128(2)), Ok($variant(3)));

                assert_eq!(
                    base.try_divide(&Bool(true)),
                    Err(ValueError::NonNumericMathOperation {
                        lhs: $variant(base),
                        operator: NumericBinaryOperator::Divide,
                        rhs: Bool(true)
                    }
                    .into())
                );
            }

            #[test]
            fn try_modulo() {
                let base: $primitive = 9;

                assert_eq!(base.try_modulo(&Decimal(Decimal::ONE)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&F32(1.0_f32)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&F64(1.0)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&I8(1)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&I16(1)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&I32(1)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&I64(1)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&I128(1)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&U8(1)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&U16(1)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&U32(1)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&U64(1)), Ok($variant(0)));
                assert_eq!(base.try_modulo(&U128(1)), Ok($variant(0)));

                assert_eq!(
                    base.try_modulo(&Bool(true)),
                    Err(ValueError::NonNumericMathOperation {
                        lhs: $variant(base),
                        operator: NumericBinaryOperator::Modulo,
                        rhs: Bool(true)
                    }
                    .into())
                );
            }
        }
    };
}

macro_rules! impl_partial_cmp_ord_method {
    ($primitive: ident) => {
        impl PartialEq<Value> for $primitive {
            fn eq(&self, other: &Value) -> bool {
                if matches!(other, Value::Bool(_)) {
                    return false;
                }

                let lhs = *self;
                let rhs = match $primitive::try_from(other) {
                    Ok(rhs) => rhs,
                    Err(_) => return false,
                };

                lhs == rhs
            }
        }

        impl PartialOrd<Value> for $primitive {
            fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
                if matches!(other, Value::Bool(_)) {
                    return None;
                }

                let lhs = self;
                let rhs = match $primitive::try_from(other) {
                    Ok(rhs) => rhs,
                    Err(_) => return None,
                };

                lhs.partial_cmp(&rhs)
            }
        }
    };
}

#[cfg(test)]
macro_rules! generate_cmp_ord_tests {
    ($primitive: ident) => {
        mod cmp_ord_tests {
            use {
                rust_decimal::prelude::Decimal, std::cmp::Ordering, $crate::data::value::Value::*,
            };

            #[test]
            fn eq() {
                let base: $primitive = 1;

                assert_eq!(base, Decimal(Decimal::ONE));
                assert_eq!(base, F32(1.0_f32));
                assert_eq!(base, F64(1.0));
                assert_eq!(base, I8(1));
                assert_eq!(base, I16(1));
                assert_eq!(base, I32(1));
                assert_eq!(base, I64(1));
                assert_eq!(base, I128(1));
                assert_eq!(base, U8(1));
                assert_eq!(base, U16(1));
                assert_eq!(base, U32(1));
                assert_eq!(base, U64(1));
                assert_eq!(base, U128(1));

                assert_ne!(base, Bool(true));
            }

            #[test]
            fn partial_cmp() {
                let base: $primitive = 1;

                assert_eq!(
                    base.partial_cmp(&Decimal(Decimal::ZERO)),
                    Some(Ordering::Greater)
                );
                assert_eq!(base.partial_cmp(&F32(0.0_f32)), Some(Ordering::Greater));
                assert_eq!(base.partial_cmp(&F64(0.0)), Some(Ordering::Greater));
                assert_eq!(base.partial_cmp(&I8(0)), Some(Ordering::Greater));
                assert_eq!(base.partial_cmp(&I16(0)), Some(Ordering::Greater));
                assert_eq!(base.partial_cmp(&I32(0)), Some(Ordering::Greater));
                assert_eq!(base.partial_cmp(&I64(0)), Some(Ordering::Greater));
                assert_eq!(base.partial_cmp(&I128(0)), Some(Ordering::Greater));
                assert_eq!(base.partial_cmp(&U8(0)), Some(Ordering::Greater));
                assert_eq!(base.partial_cmp(&U16(0)), Some(Ordering::Greater));
                assert_eq!(base.partial_cmp(&U32(0)), Some(Ordering::Greater));
                assert_eq!(base.partial_cmp(&U64(0)), Some(Ordering::Greater));
                assert_eq!(base.partial_cmp(&U128(0)), Some(Ordering::Greater));

                assert_eq!(
                    base.partial_cmp(&Decimal(Decimal::ONE)),
                    Some(Ordering::Equal)
                );
                assert_eq!(base.partial_cmp(&F32(1.0_f32)), Some(Ordering::Equal));
                assert_eq!(base.partial_cmp(&F64(1.0)), Some(Ordering::Equal));
                assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
                assert_eq!(base.partial_cmp(&I16(1)), Some(Ordering::Equal));
                assert_eq!(base.partial_cmp(&I32(1)), Some(Ordering::Equal));
                assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
                assert_eq!(base.partial_cmp(&I128(1)), Some(Ordering::Equal));
                assert_eq!(base.partial_cmp(&U8(1)), Some(Ordering::Equal));
                assert_eq!(base.partial_cmp(&U16(1)), Some(Ordering::Equal));
                assert_eq!(base.partial_cmp(&U32(1)), Some(Ordering::Equal));
                assert_eq!(base.partial_cmp(&U64(1)), Some(Ordering::Equal));
                assert_eq!(base.partial_cmp(&U128(1)), Some(Ordering::Equal));

                assert_eq!(
                    base.partial_cmp(&Decimal(Decimal::TWO)),
                    Some(Ordering::Less)
                );
                assert_eq!(base.partial_cmp(&F32(2.0_f32)), Some(Ordering::Less));
                assert_eq!(base.partial_cmp(&F64(2.0)), Some(Ordering::Less));
                assert_eq!(base.partial_cmp(&I8(2)), Some(Ordering::Less));
                assert_eq!(base.partial_cmp(&I16(2)), Some(Ordering::Less));
                assert_eq!(base.partial_cmp(&I32(2)), Some(Ordering::Less));
                assert_eq!(base.partial_cmp(&I64(2)), Some(Ordering::Less));
                assert_eq!(base.partial_cmp(&I128(2)), Some(Ordering::Less));
                assert_eq!(base.partial_cmp(&U8(2)), Some(Ordering::Less));
                assert_eq!(base.partial_cmp(&U16(2)), Some(Ordering::Less));
                assert_eq!(base.partial_cmp(&U32(2)), Some(Ordering::Less));
                assert_eq!(base.partial_cmp(&U64(2)), Some(Ordering::Less));
                assert_eq!(base.partial_cmp(&U128(2)), Some(Ordering::Less));

                assert_eq!(base.partial_cmp(&Bool(true)), None);
            }
        }
    };
}

#[cfg(test)]
pub(crate) use {generate_binary_op_tests, generate_cmp_ord_tests};
pub(crate) use {
    impl_interval_method, impl_method, impl_partial_cmp_ord_method, impl_try_binary_op,
};
