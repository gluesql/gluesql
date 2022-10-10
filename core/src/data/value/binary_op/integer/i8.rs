use crate::impl_try_binary_op;

use {crate::prelude::Value, rust_decimal::prelude::Decimal, std::cmp::Ordering};

impl_try_binary_op!(I8, i8);

impl PartialEq<Value> for i8 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => self == other,
            I16(other) => (*self as i16) == *other,
            I32(other) => (*self as i32) == *other,
            I64(other) => (*self as i64) == *other,
            I128(other) => (*self as i128) == *other,
            U8(other) => (*self as i64) == (*other as i64),
            F64(other) => ((*self as f64) - other).abs() < f64::EPSILON,
            Decimal(other) => Decimal::from(*self) == *other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for i8 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match other {
            I8(other) => self.partial_cmp(other),
            I16(other) => (*self as i16).partial_cmp(other),
            I32(other) => (*self as i32).partial_cmp(other),
            I64(other) => (*self as i64).partial_cmp(other),
            I128(other) => (*self as i128).partial_cmp(other),
            U8(other) => (*self as i64).partial_cmp(&(*other as i64)),
            F64(other) => (*self as f64).partial_cmp(other),
            Decimal(other) => Decimal::from(*self).partial_cmp(other),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::data::{
            value::{
                TryBinaryOperator,
                Value::{self, *},
            },
            NumericBinaryOperator::{self, *},
            ValueError,
        },
        rust_decimal::prelude::Decimal,
        std::cmp::Ordering,
    };

    fn overflow_err(
        lhs: Value,
        rhs: Value,
        op: NumericBinaryOperator,
    ) -> Result<Value, crate::result::Error> {
        Err(ValueError::BinaryOperationOverflow {
            lhs,
            rhs,
            operator: op,
        }
        .into())
    }

    #[test]
    fn eq_primitive() {
        assert_eq!(-1_i8, I8(-1));
        assert_eq!(0_i8, I8(0));
        assert_eq!(1_i8, I8(1));
        assert_eq!(i8::MAX, I8(i8::MAX));
        assert_eq!(i8::MIN, I8(i8::MIN));
    }

    #[test]
    fn eq() {
        let base = 1_i8;

        assert_eq!(base, I8(1));
        assert_eq!(base, I16(1));
        assert_eq!(base, I32(1));
        assert_eq!(base, I64(1));
        assert_eq!(base, I128(1));
        assert_eq!(base, U8(1));
        assert_eq!(base, F64(1.0));
        assert_eq!(base, Decimal(Decimal::ONE));

        assert_ne!(base, Bool(true));
    }

    #[test]
    fn partial_cmp() {
        let base = 1_i8;

        assert_eq!(base.partial_cmp(&I8(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I16(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I32(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I64(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&I128(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&U8(0)), Some(Ordering::Greater));
        assert_eq!(base.partial_cmp(&F64(0.0)), Some(Ordering::Greater));

        assert_eq!(base.partial_cmp(&I8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I16(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I32(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I64(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&I128(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&U8(1)), Some(Ordering::Equal));
        assert_eq!(base.partial_cmp(&F64(1.0)), Some(Ordering::Equal));

        assert_eq!(base.partial_cmp(&I8(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I16(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I32(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I64(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&I128(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&U8(2)), Some(Ordering::Less));
        assert_eq!(base.partial_cmp(&F64(2.0)), Some(Ordering::Less));

        assert_eq!(
            base.partial_cmp(&Decimal(Decimal::ONE)),
            Some(Ordering::Equal)
        );

        assert_eq!(base.partial_cmp(&Bool(true)), None);
    }

    #[test]
    fn add_overflow() {
        assert_eq!(
            i8::MAX.try_add(&Decimal(Decimal::from(1_i8))),
            overflow_err(I8(i8::MAX), Decimal(Decimal::from(1_i8)), Add)
        );

        assert_eq!(
            i8::MAX.try_add(&I8(1)),
            overflow_err(I8(i8::MAX), I8(1), Add)
        );
        assert_eq!(
            i8::MAX.try_add(&I16(1)),
            overflow_err(I8(i8::MAX), I64(1), Add)
        );
        assert_eq!(
            i8::MAX.try_add(&I32(1)),
            overflow_err(I8(i8::MAX), I32(1), Add)
        );
        assert_eq!(
            i8::MAX.try_add(&I64(1)),
            overflow_err(I8(i8::MAX), I64(1), Add)
        );
        assert_eq!(
            i8::MAX.try_add(&I128(1)),
            overflow_err(I8(i8::MAX), I128(1), Add)
        );
        assert_eq!(
            i8::MAX.try_add(&U8(1)),
            overflow_err(I8(i8::MAX), U8(1), Add)
        );
    }

    #[test]
    fn sub_overflow() {
        assert_eq!(
            i8::MIN.try_subtract(&Decimal(Decimal::from(1_i8))),
            overflow_err(I8(i8::MIN), Decimal(Decimal::from(1_i8)), Subtract)
        );

        assert_eq!(
            i8::MIN.try_subtract(&I8(1)),
            overflow_err(I8(i8::MIN), I8(1), Subtract)
        );
        assert_eq!(
            i8::MIN.try_subtract(&I16(1)),
            overflow_err(I8(i8::MIN), I16(1), Subtract)
        );
        assert_eq!(
            i8::MIN.try_subtract(&I32(1)),
            overflow_err(I8(i8::MIN), I32(1), Subtract)
        );
        assert_eq!(
            i8::MIN.try_subtract(&I64(1)),
            overflow_err(I8(i8::MIN), I64(1), Subtract)
        );
        assert_eq!(
            i8::MIN.try_subtract(&I128(1)),
            overflow_err(I8(i8::MIN), I128(1), Subtract)
        );
        assert_eq!(
            i8::MIN.try_subtract(&U8(1)),
            overflow_err(I8(i8::MIN), U8(1), Subtract)
        );
    }

    #[test]
    fn mul_overflow() {
        assert_eq!(
            i8::MAX.try_multiply(&Decimal(Decimal::from(2_i8))),
            overflow_err(I8(i8::MAX), Decimal(Decimal::from(2_i8)), Multiply)
        );

        assert_eq!(
            i8::MAX.try_multiply(&I8(2)),
            overflow_err(I8(i8::MAX), I8(2), Multiply)
        );
        assert_eq!(
            i8::MAX.try_multiply(&I16(2)),
            overflow_err(I8(i8::MAX), I16(2), Multiply)
        );
        assert_eq!(
            i8::MAX.try_multiply(&I32(2)),
            overflow_err(I8(i8::MAX), I32(2), Multiply)
        );
        assert_eq!(
            i8::MAX.try_multiply(&I64(2)),
            overflow_err(I8(i8::MAX), I64(2), Multiply)
        );
        assert_eq!(
            i8::MAX.try_multiply(&I128(2)),
            overflow_err(I8(i8::MAX), I128(2), Multiply)
        );
        assert_eq!(
            i8::MAX.try_multiply(&U8(2)),
            overflow_err(I8(i8::MAX), U8(2), Multiply)
        );
    }

    #[test]
    fn div_overflow() {
        // TODO: handle zero divide error
        assert_eq!(
            i8::MAX.try_divide(&Decimal(Decimal::from(0_i8))),
            overflow_err(I8(i8::MAX), Decimal(Decimal::from(0_i8)), Divide)
        );

        assert_eq!(
            i8::MAX.try_divide(&I8(0)),
            overflow_err(I8(i8::MAX), I8(0), Divide)
        );
        assert_eq!(
            i8::MAX.try_divide(&I16(0)),
            overflow_err(I8(i8::MAX), I16(0), Divide)
        );
        assert_eq!(
            i8::MAX.try_divide(&I32(0)),
            overflow_err(I8(i8::MAX), I32(0), Divide)
        );
        assert_eq!(
            i8::MAX.try_divide(&I64(0)),
            overflow_err(I8(i8::MAX), I64(0), Divide)
        );
        assert_eq!(
            i8::MAX.try_divide(&I128(0)),
            overflow_err(I8(i8::MAX), I128(0), Divide)
        );
        assert_eq!(
            i8::MAX.try_divide(&U8(0)),
            overflow_err(I8(i8::MAX), U8(0), Divide)
        );
    }

    #[test]
    fn mod_overlfow() {
        assert_eq!(
            i8::MAX.try_modulo(&Decimal(Decimal::from(0_i8))),
            overflow_err(I8(i8::MAX), Decimal(Decimal::from(0_i8)), Modulo)
        );

        assert_eq!(
            i8::MAX.try_modulo(&I8(0)),
            overflow_err(I8(i8::MAX), I8(0), Modulo)
        );
        assert_eq!(
            i8::MAX.try_modulo(&I16(0)),
            overflow_err(I8(i8::MAX), I16(0), Modulo)
        );
        assert_eq!(
            i8::MAX.try_modulo(&I32(0)),
            overflow_err(I8(i8::MAX), I32(0), Modulo)
        );
        assert_eq!(
            i8::MAX.try_modulo(&I64(0)),
            overflow_err(I8(i8::MAX), I64(0), Modulo)
        );
        assert_eq!(
            i8::MAX.try_modulo(&I128(0)),
            overflow_err(I8(i8::MAX), I128(0), Modulo)
        );
        assert_eq!(
            i8::MAX.try_modulo(&U8(0)),
            overflow_err(I8(i8::MAX), U8(0), Modulo)
        );
    }

    #[test]
    fn try_add() {
        let base = 1_i8;

        assert_eq!(base.try_add(&Decimal(Decimal::ONE)), Ok(I8(2)));
        assert_eq!(base.try_add(&F64(1.0)), Ok(I8(2)));
        assert_eq!(base.try_add(&I8(1)), Ok(I8(2)));
        assert_eq!(base.try_add(&I16(1)), Ok(I8(2)));
        assert_eq!(base.try_add(&I32(1)), Ok(I8(2)));
        assert_eq!(base.try_add(&I64(1)), Ok(I8(2)));
        assert_eq!(base.try_add(&I128(1)), Ok(I8(2)));
        assert_eq!(base.try_add(&U8(1)), Ok(I8(2)));

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

        assert_eq!(base.try_subtract(&Decimal(Decimal::ONE)), Ok(I8(0)));
        assert_eq!(base.try_subtract(&F64(1.0)), Ok(I8(0)));
        assert_eq!(base.try_subtract(&I8(1)), Ok(I8(0)));
        assert_eq!(base.try_subtract(&I16(1)), Ok(I8(0)));
        assert_eq!(base.try_subtract(&I32(1)), Ok(I8(0)));
        assert_eq!(base.try_subtract(&I64(1)), Ok(I8(0)));
        assert_eq!(base.try_subtract(&I128(1)), Ok(I8(0)));
        assert_eq!(base.try_subtract(&U8(1)), Ok(I8(0)));

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

        assert_eq!(base.try_multiply(&Decimal(Decimal::TWO)), Ok(I8(6)));
        assert_eq!(base.try_multiply(&F64(2.0)), Ok(I8(6)));
        assert_eq!(base.try_multiply(&I8(2)), Ok(I8(6)));
        assert_eq!(base.try_multiply(&I16(2)), Ok(I16(6)));
        assert_eq!(base.try_multiply(&I32(2)), Ok(I32(6)));
        assert_eq!(base.try_multiply(&I64(2)), Ok(I64(6)));
        assert_eq!(base.try_multiply(&I128(2)), Ok(I128(6)));
        assert_eq!(base.try_multiply(&U8(2)), Ok(U8(6)));

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

        assert_eq!(base.try_divide(&Decimal(Decimal::TWO)), Ok(I8(3)));
        assert_eq!(base.try_divide(&F64(2.0)), Ok(I8(3)));
        assert_eq!(base.try_divide(&I8(2)), Ok(I8(3)));
        assert_eq!(base.try_divide(&I16(2)), Ok(I8(3)));
        assert_eq!(base.try_divide(&I32(2)), Ok(I8(3)));
        assert_eq!(base.try_divide(&I64(2)), Ok(I8(3)));
        assert_eq!(base.try_divide(&I128(2)), Ok(I8(3)));
        assert_eq!(base.try_divide(&U8(2)), Ok(I8(3)));

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

        assert_eq!(base.try_modulo(&Decimal(Decimal::ONE)), Ok(I8(0)));
        assert_eq!(base.try_modulo(&F64(1.0)), Ok(I8(0)));
        assert_eq!(base.try_modulo(&I8(1)), Ok(I8(0)));
        assert_eq!(base.try_modulo(&I16(1)), Ok(I8(0)));
        assert_eq!(base.try_modulo(&I32(1)), Ok(I8(0)));
        assert_eq!(base.try_modulo(&I64(1)), Ok(I8(0)));
        assert_eq!(base.try_modulo(&I128(1)), Ok(I8(0)));
        assert_eq!(base.try_modulo(&U8(1)), Ok(I8(0)));

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
