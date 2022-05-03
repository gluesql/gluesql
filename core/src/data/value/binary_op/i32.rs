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
        Ok(F64(lhs as f64))
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
