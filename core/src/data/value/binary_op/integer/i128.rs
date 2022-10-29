use {crate::prelude::Value, rust_decimal::prelude::Decimal, std::cmp::Ordering};

super::macros::impl_try_binary_op!(I128, i128);
#[cfg(test)]
super::macros::generate_binary_op_tests!(I128, i128);

impl PartialEq<Value> for i128 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            I8(other) => (*self as i8) == *other,
            I16(other) => (*self as i16) == *other,
            I32(other) => (*self as i32) == *other,
            I64(other) => (*self as i64) == *other,
            I128(other) => self == other,
            U8(other) => *self == (*other as i128),
            U16(other) => *self == (*other as i128),
            F64(other) => ((*self as f64) - other).abs() < f64::EPSILON,
            Decimal(other) => Decimal::from(*self) == *other,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for i128 {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match other {
            I8(other) => (*self as i8).partial_cmp(other),
            I16(other) => (*self as i16).partial_cmp(other),
            I32(other) => (*self as i32).partial_cmp(other),
            I64(other) => (*self as i64).partial_cmp(other),
            I128(other) => self.partial_cmp(other),
            U8(other) => self.partial_cmp(&(*other as i128)),
            U16(other) => self.partial_cmp(&(*other as i128)),
            F64(other) => (*self as f64).partial_cmp(other),
            Decimal(other) => Decimal::from(*self).partial_cmp(other),
            _ => None,
        }
    }
}
