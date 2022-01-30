use {
    super::{Value, ValueError},
    crate::result::Result,
    bigdecimal::BigDecimal,
    bigdecimal::FromPrimitive,
    bigdecimal::ToPrimitive,
    rust_decimal::Decimal,
    std::borrow::Cow,
};


impl Value {
    pub fn parse_decimal(precision: &Option<u64>, scale: &Option<u64>, value: &Cow<'_, BigDecimal>) -> Result<Value> {
        
        println!("precision: {:?}, scale: {:?}, value: {:?}", precision, scale, value);

        match (precision, scale) {
            (None, None) => return value
                .to_string()
                .parse::<Decimal>()
                .map(Value::Decimal)
                .map_err(|_| ValueError::FailedToParseDecimal(value.to_string()).into()),
            (Some(p), None) => {
                if value.with_scale(0).to_u64().unwrap() > 10_u64.pow((*p).try_into().unwrap()) {
                    println!("left: {}, right: {}",value.with_scale(0).to_u64().unwrap(), 10^*p);
                    return Err(ValueError::FailedToParseDecimal(value.to_string()).into());
                }
                return value.with_prec(*p).with_scale(0)
                    .to_string()
                    .parse::<Decimal>()
                    .map(Value::Decimal)
                    .map_err(|_| ValueError::FailedToParseDecimal(value.to_string()).into())},
            (Some(p), Some(s)) => {
                // println!("value prec,scaled: {:?}", value.with_prec(*p).with_scale(*s as i64));
                // println!("p: {}, *p, {:?}",p,*p);
                if value.with_scale(0).to_u64().unwrap() > 10_u64.pow((*p - *s).try_into().unwrap()) {
                    println!("digits: {}, p - s: {}", value.digits(), (*p - *s));
                    return Err(ValueError::FailedToParseDecimal(value.to_string()).into());
                }
                return value.with_prec(*p).with_scale(*s as i64)
                    .to_string()
                    .parse::<Decimal>()
                    .map(Value::Decimal)
                    .map_err(|_| ValueError::FailedToParseDecimal(value.to_string()).into())},
            (None, Some(_)) => return Err(ValueError::NoPrecisionDecimalNotSupported.into())
        }
    }

}


