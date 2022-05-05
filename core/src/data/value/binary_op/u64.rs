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

impl PartialEq<Value> for u64 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => &(*self as i128) == &(*other as i128),
            I32(other) => &(*self as i128) == &(*other as i128),
            I64(other) => &(*self as i128) == &(*other as i128),
            I128(other) => &(*self as i128) == other,
            I32(other) => &(*self as i64) == &(*other as i64),
            U8(other) => self == &(*other as u64),
            U32(other) => self == &(*other as u64),
            U64(other) => self  == other,
            U128(other) => &(*self as u128) == other,
            F64(other) => (*self as f64) == *other,
            Decimal(other) => Decimal::from(*self) == *other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for u64 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match other {
            I8(other) => PartialOrd::partial_cmp(&(*self as i128), &(*other as i128)),
            I32(other) => PartialOrd::partial_cmp(&(*self as i128), &(*other as i128)),
            I64(other) => PartialOrd::partial_cmp(&(*self as i128), &(*other as i128)),
            I128(other) => PartialOrd::partial_cmp(&(*self as i128), other),
            U8(other) => PartialOrd::partial_cmp(self, &(*other as u64)),
            U32(other) => PartialOrd::partial_cmp(self, &(*other as u64)),
            U64(other) => PartialOrd::partial_cmp(self, other),
            U128(other) => PartialOrd::partial_cmp(&(*self as u128), other),
            F64(other) => PartialOrd::partial_cmp(&(*self as f64), other),
            Decimal(other) => Decimal::from(*self).partial_cmp(other),
            _ => None,
        }
    }
}
