use bigdecimal::BigDecimal;

pub trait BigDecimalExt {
    fn to_i8(&self) -> Option<i8>;
    fn to_i64(&self) -> Option<i64>;
    fn to_f64(&self) -> Option<f64>;
}

impl BigDecimalExt for BigDecimal {
    fn to_i8(&self) -> Option<i8> {
        self.is_integer()
            .then(|| bigdecimal::ToPrimitive::to_i8(self))?
    }
    
    fn to_i64(&self) -> Option<i64> {
        match self.is_integer() {
            true => bigdecimal::ToPrimitive::to_i64(self),
            false => None,
        }
    }

    fn to_f64(&self) -> Option<f64> {
        bigdecimal::ToPrimitive::to_f64(self)
    }
}
