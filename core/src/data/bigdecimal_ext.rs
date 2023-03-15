use bigdecimal::BigDecimal;

pub trait BigDecimalExt {
    fn to_i8(&self) -> Option<i8>;
    fn to_i16(&self) -> Option<i16>;
    fn to_i32(&self) -> Option<i32>;
    fn to_i64(&self) -> Option<i64>;
    fn to_i128(&self) -> Option<i128>;
    fn to_u8(&self) -> Option<u8>;
    fn to_u16(&self) -> Option<u16>;
    fn to_u32(&self) -> Option<u32>;
    fn to_u128(&self) -> Option<u128>;
    fn to_f64(&self) -> Option<f64>;
}

impl BigDecimalExt for BigDecimal {
    fn to_i8(&self) -> Option<i8> {
        self.is_integer()
            .then(|| bigdecimal::ToPrimitive::to_i8(self))?
    }
    fn to_i16(&self) -> Option<i16> {
        self.is_integer()
            .then(|| bigdecimal::ToPrimitive::to_i16(self))?
    }
    fn to_i32(&self) -> Option<i32> {
        self.is_integer()
            .then(|| bigdecimal::ToPrimitive::to_i32(self))?
    }
    fn to_i64(&self) -> Option<i64> {
        match self.is_integer() {
            true => bigdecimal::ToPrimitive::to_i64(self),
            false => None,
        }
    }
    fn to_i128(&self) -> Option<i128> {
        match self.is_integer() {
            true => bigdecimal::ToPrimitive::to_i128(self),
            false => None,
        }
    }
    fn to_u8(&self) -> Option<u8> {
        self.is_integer()
            .then(|| bigdecimal::ToPrimitive::to_u8(self))?
    }
    fn to_u16(&self) -> Option<u16> {
        self.is_integer()
            .then(|| bigdecimal::ToPrimitive::to_u16(self))?
    }
    fn to_u32(&self) -> Option<u32> {
        self.is_integer()
            .then(|| bigdecimal::ToPrimitive::to_u32(self))?
    }
    fn to_u128(&self) -> Option<u128> {
        self.is_integer()
            .then(|| bigdecimal::ToPrimitive::to_u128(self))?
    }
    fn to_f64(&self) -> Option<f64> {
        bigdecimal::ToPrimitive::to_f64(self)
    }
}
