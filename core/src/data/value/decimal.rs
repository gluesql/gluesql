use {
    super::{Value, ValueError},
    crate::result::Result,
    bigdecimal::BigDecimal,
    bigdecimal::FromPrimitive,
    rust_decimal::Decimal,
    std::borrow::Cow,
};


impl Value {
    pub fn parse_decimal(precision: &Option<u64>, scale: &Option<u64>, value: &Cow<'_, BigDecimal>) -> Result<Value> {
        // let value = value.to_string()
        //     .parse::<Decimal>()
        //     .map(Value::Decimal)
        //     .map_err(|_| ValueError::FailedToParseDecimal(value.to_string()).into())?;
        println!("precision: {:?}, scale: {:?}, value: {:?}", precision, scale, value);
        // let t = value;

        match (precision, scale) {
            (None, None) => return value
                .to_string()
                .parse::<Decimal>()
                .map(Value::Decimal)
                .map_err(|_| ValueError::FailedToParseDecimal(value.to_string()).into()),
            (Some(p), None) => {
                // if value.is_integer() {

                // }
                // if t > BigDecimal::from_u64(10^(*p)).unwrap() {
                //     return Err(ValueError::FailedToParseDecimal(value.to_string()).into())
                // }
                // println!("cut! : {:?}", value.with_scale(0));
                // println!("cut! : {:?}", value.with_prec(*p));
                if value.digits() > *p {
                    return Err(ValueError::FailedToParseDecimal(value.to_string()).into());
                }
                return value.with_prec(*p).with_scale(0)
                .to_string()
                .parse::<Decimal>()
                .map(Value::Decimal)
                .map_err(|_| ValueError::FailedToParseDecimal(value.to_string()).into())},
            (Some(p), Some(s)) => return value.with_prec(*p).with_scale(*s as i64)
                .to_string()
                .parse::<Decimal>()
                .map(Value::Decimal)
                .map_err(|_| ValueError::FailedToParseDecimal(value.to_string()).into()),
            (None, Some(_)) => return Err(ValueError::NoPrecisionDecimalNotSupported.into())
        }
    }

}


