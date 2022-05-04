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

impl PartialEq<Value> for i32 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => self == &(*other as i32),
            I32(other) => self == other,
            I64(other) => &(*self as i64) == other,
            I128(other) => &(*self as i128) == other,
            U8(other) => self == &(*other as i32),
            U32(other) => (*self as i64) == (*other as i64),
            U64(other) => (*self as i128) == (*other as i128),
            U128(other) => (*self as i128) == (*other as i128),
            F64(other) => (*self as f64) == *other,
            Decimal(other) => Decimal::from(*self) == *other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for i32 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match other {
            I8(other) => PartialOrd::partial_cmp(self, &(*other as i32)),
            I32(other) => PartialOrd::partial_cmp(self, other),
            I64(other) => PartialOrd::partial_cmp(&(*self as i64), other),
            I128(other) => PartialOrd::partial_cmp(&(*self as i128), other),
            U8(other) => PartialOrd::partial_cmp(self, &(*other as i32)),
            U32(other) => PartialOrd::partial_cmp(&(*self as i64), &(*other as i64)),
            U64(other) => PartialOrd::partial_cmp(&(*self as i128), &(*other as i128)),
            U128(other) => PartialOrd::partial_cmp(&(*self as i128), &(*other as i128)),
            F64(other) => PartialOrd::partial_cmp(&(*self as f64), other),
            Decimal(other) => Decimal::from(*self).partial_cmp(other),
            _ => None,
        }
    }
}

impl TryBinaryOperator for i32 {
    type Rhs = Value;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;

        match *rhs {
            I8(rhs) => lhs
                .checked_add(rhs as i32)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I32(lhs),
                        rhs: I8(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I32),
            I32(rhs) => lhs
                .checked_add(rhs)
                .ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I32(lhs),
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
                        lhs: I32(lhs),
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
                        lhs: I32(lhs),
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
                        lhs: I32(lhs),
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
                        lhs: I32(lhs),
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
                        lhs: I32(lhs),
                        rhs: U64(rhs),
                        operator: NumericBinaryOperator::Add,
                    }
                    .into()
                })
                .map(I128),
            U128(rhs) => match i128::try_from(rhs) {
                Ok(x) => (lhs as i128).checked_add(x).map(I128).ok_or_else(|| {
                    ValueError::BinaryOperationOverflow {
                        lhs: I32(lhs),
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
                lhs: I32(lhs),
                operator: NumericBinaryOperator::Add,
                rhs: rhs.clone(),
            }
            .into()),
        }
    }
    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;
        Ok(F64(lhs as f64))
    }
    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;
        Ok(F64(lhs as f64))
    }
    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;
        Ok(F64(lhs as f64))
    }
    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
        let lhs = *self;
        Ok(F64(lhs as f64))
    }
}
