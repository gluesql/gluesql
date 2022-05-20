use {
    super::{Value, ValueError},
    crate::result::Result,
    bigdecimal::BigDecimal,
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
            (Some(p), s) => {
                let s: u64 = match s {
                    Some(s) => *s,
                    None => 0,
                };
                let new_value = value.round(s as i64);
                if new_value.digits() > *p {
                    return Err(ValueError::FailedToParseDecimal(value.to_string()).into());
                }
                bigdecimal_literal_to_decimal_value(&new_value.with_prec(*p))
            }
            (None, Some(_)) => Err(ValueError::NoPrecisionDecimalNotSupported.into()),
        }
    }
}
