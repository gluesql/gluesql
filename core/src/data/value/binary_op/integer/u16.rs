use {
    crate::{impl_try_binary_op, prelude::Value},
    rust_decimal::prelude::Decimal,
    std::cmp::Ordering,
};

impl_try_binary_op!(U16, u16);
#[cfg(test)]
crate::generate_binary_op_tests!(U16, u16);

impl PartialEq<Value> for u16 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => (*self as i16) == (*other as i16),
            I16(other) => (*self as i16) == *other,
            I32(other) => (*self as i32) == *other,
            I64(other) => (*self as i64) == *other,
            I128(other) => (*self as i128) == *other,
            U8(other) => (*self as u8) == *other,
            U16(other) => self == other,
            F64(other) => ((*self as f64) - other).abs() < f64::EPSILON,
            Decimal(other) => Decimal::from(*self) == *other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for u16 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match other {
            I8(other) => (*self as i16).partial_cmp(&(*other as i16)),
            I16(other) => (*self as i16).partial_cmp(other),
            I32(other) => (*self as i32).partial_cmp(other),
            I64(other) => (*self as i64).partial_cmp(other),
            I128(other) => (*self as i128).partial_cmp(other),
            U8(other) => (*self as u8).partial_cmp(other),
            U16(other) => self.partial_cmp(other),
            F64(other) => (*self as f64).partial_cmp(other),
            Decimal(other) => Decimal::from(*self).partial_cmp(other),
            _ => None,
        }
    }
}
