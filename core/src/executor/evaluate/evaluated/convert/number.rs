use {
    super::LiteralError,
    crate::{
        ast::DataType,
        data::{BigDecimalExt, Value},
        result::Result,
    },
    bigdecimal::BigDecimal,
    rust_decimal::Decimal,
    std::net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

fn parse_failed(literal: &BigDecimal, data_type: &DataType) -> LiteralError {
    LiteralError::NumberParseFailed {
        literal: literal.to_string(),
        data_type: data_type.clone(),
    }
}

fn cast_failed(literal: &BigDecimal, data_type: &DataType) -> LiteralError {
    LiteralError::NumberCastFailed {
        literal: literal.to_string(),
        data_type: data_type.clone(),
    }
}

pub(crate) fn number_to_value(data_type: &DataType, value: &BigDecimal) -> Result<Value> {
    match data_type {
        DataType::Int8 => value
            .to_i8()
            .map(Value::I8)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Int16 => value
            .to_i16()
            .map(Value::I16)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Int32 => value
            .to_i32()
            .map(Value::I32)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Int => value
            .to_i64()
            .map(Value::I64)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Int128 => value
            .to_i128()
            .map(Value::I128)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Uint8 => value
            .to_u8()
            .map(Value::U8)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Uint16 => value
            .to_u16()
            .map(Value::U16)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Uint32 => value
            .to_u32()
            .map(Value::U32)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Uint64 => value
            .to_u64()
            .map(Value::U64)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Uint128 => value
            .to_u128()
            .map(Value::U128)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Float32 => value
            .to_f32()
            .map(Value::F32)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Float => value
            .to_f64()
            .map(Value::F64)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Inet => {
            if let Some(v4) = value.to_u32() {
                Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(v4))))
            } else {
                value
                    .to_u128()
                    .map(|v6| Value::Inet(IpAddr::V6(Ipv6Addr::from(v6))))
                    .ok_or_else(|| parse_failed(value, data_type).into())
            }
        }
        DataType::Decimal => value
            .to_string()
            .parse::<Decimal>()
            .map(Value::Decimal)
            .map_err(|_| parse_failed(value, data_type).into()),
        _ => Err(parse_failed(value, data_type).into()),
    }
}

pub(crate) fn cast_number_to_value(data_type: &DataType, value: &BigDecimal) -> Result<Value> {
    match data_type {
        DataType::Boolean => match value.to_i64() {
            Some(0) => Ok(Value::Bool(false)),
            Some(1) => Ok(Value::Bool(true)),
            _ => Err(cast_failed(value, data_type).into()),
        },
        DataType::Text => Ok(Value::Str(value.to_string())),
        _ => number_to_value(data_type, value),
    }
}

#[cfg(test)]
mod tests {
    use super::{cast_number_to_value, number_to_value};
    use crate::{ast::DataType, data::Value, error::LiteralError};
    use bigdecimal::BigDecimal;
    use rust_decimal::Decimal;
    use std::{
        net::{IpAddr, Ipv4Addr},
        str::FromStr,
    };

    fn dec(value: &str) -> BigDecimal {
        BigDecimal::from_str(value).unwrap()
    }

    #[test]
    fn test_number_to_value() {
        assert_eq!(
            number_to_value(&DataType::Int, &dec("123")),
            Ok(Value::I64(123))
        );
        assert_eq!(
            number_to_value(&DataType::Int8, &dec("64")),
            Ok(Value::I8(64))
        );
        assert_eq!(
            number_to_value(&DataType::Int, &dec("1.5")),
            Err(LiteralError::NumberParseFailed {
                literal: "1.5".to_owned(),
                data_type: DataType::Int
            }
            .into())
        );
        assert_eq!(
            number_to_value(&DataType::Float32, &dec("1.5")),
            Ok(Value::F32(1.5))
        );
        assert_eq!(
            number_to_value(&DataType::Decimal, &dec("200")),
            Ok(Value::Decimal(Decimal::new(200, 0)))
        );
        assert_eq!(
            number_to_value(&DataType::Inet, &dec("4294967295")),
            Ok(Value::Inet(IpAddr::V4(Ipv4Addr::BROADCAST)))
        );
        assert_eq!(
            number_to_value(&DataType::Inet, &dec("-1")),
            Err(LiteralError::NumberParseFailed {
                literal: "-1".to_owned(),
                data_type: DataType::Inet
            }
            .into())
        );
    }

    #[test]
    fn test_cast_number_to_value() {
        assert_eq!(
            cast_number_to_value(&DataType::Boolean, &dec("0")),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            cast_number_to_value(&DataType::Boolean, &dec("1")),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            cast_number_to_value(&DataType::Boolean, &dec("2")),
            Err(LiteralError::NumberCastFailed {
                literal: "2".to_owned(),
                data_type: DataType::Boolean
            }
            .into())
        );
        assert_eq!(
            cast_number_to_value(&DataType::Text, &dec("42")),
            Ok(Value::Str("42".to_owned()))
        );
        assert_eq!(
            cast_number_to_value(&DataType::Int8, &dec("64")),
            Ok(Value::I8(64))
        );
    }
}
