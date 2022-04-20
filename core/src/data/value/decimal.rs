use {
    super::{Value, ValueError},
    crate::result::Result,
    bigdecimal::BigDecimal,
    bigdecimal::ToPrimitive,
    rust_decimal::Decimal,
};

fn bigdecimal_literal_to_decimal_value(value: &BigDecimal) -> Result<Value> {
    value
        .to_string()
        .parse::<Decimal>()
        .map(Value::Decimal)
        .map_err(|_| ValueError::FailedToParseDecimal(value.to_string()).into())
}

impl Value {
    pub fn parse_decimal(
        precision: &Option<u64>,
        scale: &Option<u64>,
        value: &BigDecimal,
    ) -> Result<Value> {
        match (precision, scale) {
            (None, None) => bigdecimal_literal_to_decimal_value(value),
            (Some(p), None) => {
                if value.with_scale(0).to_u64().unwrap() > 10_u64.pow((*p).try_into().unwrap()) {
                    return Err(ValueError::FailedToParseDecimal(value.to_string()).into());
                }
                bigdecimal_literal_to_decimal_value(&value.with_prec(*p).with_scale(0))
            }
            (Some(p), Some(s)) => {
                if value.with_scale(0).to_u64().unwrap() > 10_u64.pow((*p - *s).try_into().unwrap())
                {
                    return Err(ValueError::FailedToParseDecimal(value.to_string()).into());
                }
                bigdecimal_literal_to_decimal_value(&value.with_prec(*p).with_scale(*s as i64))
            }
            (None, Some(_)) => Err(ValueError::NoPrecisionDecimalNotSupported.into()),
        }
    }
}
