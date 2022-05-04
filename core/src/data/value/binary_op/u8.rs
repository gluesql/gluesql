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
            I8(other) => &(*self as i16) == &(*other as i16),
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
