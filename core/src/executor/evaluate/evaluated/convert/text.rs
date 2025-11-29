use {
    crate::{
        ast::DataType,
        data::{
            Interval, Point, Value,
            value::{parse_date, parse_time, parse_timestamp, parse_uuid},
        },
        executor::EvaluateError,
        result::Result,
    },
    rust_decimal::Decimal,
    std::{net::IpAddr, str::FromStr},
};

fn parse_failed(literal: &str, data_type: &DataType) -> EvaluateError {
    EvaluateError::TextParseFailed {
        literal: literal.to_owned(),
        data_type: data_type.clone(),
    }
}

fn cast_failed(literal: &str, data_type: &DataType) -> EvaluateError {
    EvaluateError::TextCastFailed {
        literal: literal.to_owned(),
        data_type: data_type.clone(),
    }
}

pub(crate) fn text_to_value(data_type: &DataType, value: &str) -> Result<Value> {
    match data_type {
        DataType::Text => Ok(Value::Str(value.to_owned())),
        DataType::Bytea => hex::decode(value)
            .map(Value::Bytea)
            .map_err(|_| parse_failed(value, data_type).into()),
        DataType::Inet => IpAddr::from_str(value)
            .map(Value::Inet)
            .map_err(|_| parse_failed(value, data_type).into()),
        DataType::Interval => Interval::parse(value).map(Value::Interval),
        DataType::Point => Point::from_wkt(value)
            .map(Value::Point)
            .map_err(|_| parse_failed(value, data_type).into()),
        DataType::Date => parse_date(value)
            .map(Value::Date)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Timestamp => parse_timestamp(value)
            .map(Value::Timestamp)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Time => parse_time(value)
            .map(Value::Time)
            .ok_or_else(|| parse_failed(value, data_type).into()),
        DataType::Uuid => parse_uuid(value).map(Value::Uuid),
        DataType::Map => Value::parse_json_map(value),
        DataType::List => Value::parse_json_list(value),
        _ => Err(parse_failed(value, data_type).into()),
    }
}

pub(crate) fn cast_text_to_value(data_type: &DataType, value: &str) -> Result<Value> {
    match data_type {
        DataType::Boolean => match value.to_uppercase().as_str() {
            "TRUE" | "1" => Ok(Value::Bool(true)),
            "FALSE" | "0" => Ok(Value::Bool(false)),
            _ => Err(cast_failed(value, data_type).into()),
        },
        DataType::Int8 => value
            .parse::<i8>()
            .map(Value::I8)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Int16 => value
            .parse::<i16>()
            .map(Value::I16)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Int32 => value
            .parse::<i32>()
            .map(Value::I32)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Int => value
            .parse::<i64>()
            .map(Value::I64)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Int128 => value
            .parse::<i128>()
            .map(Value::I128)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Uint8 => value
            .parse::<u8>()
            .map(Value::U8)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Uint16 => value
            .parse::<u16>()
            .map(Value::U16)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Uint32 => value
            .parse::<u32>()
            .map(Value::U32)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Uint64 => value
            .parse::<u64>()
            .map(Value::U64)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Uint128 => value
            .parse::<u128>()
            .map(Value::U128)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Float32 => value
            .parse::<f32>()
            .map(Value::F32)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Float => value
            .parse::<f64>()
            .map(Value::F64)
            .map_err(|_| cast_failed(value, data_type).into()),
        DataType::Decimal => value
            .parse::<Decimal>()
            .map(Value::Decimal)
            .map_err(|_| cast_failed(value, data_type).into()),
        _ => text_to_value(data_type, value),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{cast_text_to_value, parse_time, parse_timestamp, text_to_value},
        crate::{ast::DataType, data::Value, executor::EvaluateError},
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        rust_decimal::Decimal,
        std::{net::IpAddr, str::FromStr},
    };

    fn date_time(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32, ms: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_milli_opt(hh, mm, ss, ms)
            .unwrap()
    }

    fn time(hour: u32, min: u32, sec: u32, milli: u32) -> NaiveTime {
        NaiveTime::from_hms_milli_opt(hour, min, sec, milli).unwrap()
    }

    #[test]
    fn timestamp_literal() {
        macro_rules! test (
            ($timestamp: literal, $result: expr) => {
                assert_eq!(parse_timestamp($timestamp), Some($result));
            }
        );

        test!("2022-12-20T10:00:00Z", date_time(2022, 12, 20, 10, 0, 0, 0));
        test!(
            "2022-12-20T10:00:00.132Z",
            date_time(2022, 12, 20, 10, 0, 0, 132)
        );
        test!(
            "2022-12-20T10:00:00.132+09:00",
            date_time(2022, 12, 20, 1, 0, 0, 132)
        );
        test!("2022-11-21", date_time(2022, 11, 21, 0, 0, 0, 0));
        test!("2022-12-20T10:00:00", date_time(2022, 12, 20, 10, 0, 0, 0));
        test!("2022-12-20 10:00:00Z", date_time(2022, 12, 20, 10, 0, 0, 0));
        test!("2022-12-20 10:00:00", date_time(2022, 12, 20, 10, 0, 0, 0));
        test!(
            "2022-12-20 10:00:00.987",
            date_time(2022, 12, 20, 10, 0, 0, 987)
        );
    }

    #[test]
    fn time_literal() {
        macro_rules! test (
            ($time: literal, $result: expr) => {
                assert_eq!(parse_time($time), Some($result));
            }
        );

        test!("12:00:35", time(12, 0, 35, 0));
        test!("12:00:35.917", time(12, 0, 35, 917));
        test!("AM 08:00", time(8, 0, 0, 0));
        test!("PM 8:00", time(20, 0, 0, 0));
        test!("AM 09:30:37", time(9, 30, 37, 0));
        test!("PM 3:30:37", time(15, 30, 37, 0));
        test!("PM 03:30:37.123", time(15, 30, 37, 123));
        test!("AM 9:30:37.917", time(9, 30, 37, 917));
        test!("08:00 AM", time(8, 0, 0, 0));
        test!("8:00 PM", time(20, 0, 0, 0));
        test!("09:30:37 AM", time(9, 30, 37, 0));
        test!("3:30:37 PM", time(15, 30, 37, 0));
        test!("03:30:37.123 PM", time(15, 30, 37, 123));
        test!("9:30:37.917 AM", time(9, 30, 37, 917));
    }

    #[test]
    fn test_text_to_value() {
        assert_eq!(
            text_to_value(&DataType::Text, "hello"),
            Ok(Value::Str("hello".to_owned()))
        );
        assert_eq!(
            text_to_value(&DataType::Bytea, "1234"),
            Ok(Value::Bytea(hex::decode("1234").unwrap()))
        );
        assert_eq!(
            text_to_value(&DataType::Bytea, "123"),
            Err(EvaluateError::TextParseFailed {
                literal: "123".to_owned(),
                data_type: DataType::Bytea
            }
            .into())
        );
        assert_eq!(
            text_to_value(&DataType::Inet, "127.0.0.1"),
            Ok(Value::Inet(IpAddr::from_str("127.0.0.1").unwrap()))
        );
        assert_eq!(
            text_to_value(&DataType::Inet, "not-an-ip"),
            Err(EvaluateError::TextParseFailed {
                literal: "not-an-ip".to_owned(),
                data_type: DataType::Inet
            }
            .into())
        );
        assert_eq!(
            text_to_value(&DataType::Date, "2015-09-05"),
            Ok(Value::Date(NaiveDate::from_ymd_opt(2015, 9, 5).unwrap()))
        );
        assert_eq!(
            text_to_value(&DataType::Time, "07:12:23"),
            Ok(Value::Time(
                NaiveTime::from_hms_milli_opt(7, 12, 23, 0).unwrap()
            ))
        );
        assert_eq!(
            text_to_value(&DataType::Timestamp, "2022-12-20 10:00:00.987"),
            Ok(Value::Timestamp(date_time(2022, 12, 20, 10, 0, 0, 987)))
        );
        assert_eq!(
            text_to_value(&DataType::Uuid, "936DA01F9ABD4d9d80C702AF85C822A8"),
            Ok(Value::Uuid(
                195_965_723_427_462_096_757_863_453_463_987_888_808
            ))
        );
        assert_eq!(
            text_to_value(&DataType::Map, r#"{ "a": 1 }"#),
            Value::parse_json_map(r#"{ "a": 1 }"#)
        );
        assert_eq!(
            text_to_value(&DataType::List, r"[ 1, 2, 3 ]"),
            Value::parse_json_list(r"[ 1, 2, 3 ]")
        );
        assert_eq!(
            text_to_value(&DataType::Int, "123"),
            Err(EvaluateError::TextParseFailed {
                literal: "123".to_owned(),
                data_type: DataType::Int
            }
            .into())
        );
    }

    #[test]
    fn test_cast_text_to_value() {
        assert_eq!(
            cast_text_to_value(&DataType::Boolean, "true"),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            cast_text_to_value(&DataType::Boolean, "0"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            cast_text_to_value(&DataType::Boolean, "maybe"),
            Err(EvaluateError::TextCastFailed {
                literal: "maybe".to_owned(),
                data_type: DataType::Boolean
            }
            .into())
        );
        assert_eq!(
            cast_text_to_value(&DataType::Int16, "127"),
            Ok(Value::I16(127))
        );
        assert_eq!(
            cast_text_to_value(&DataType::Int16, "abc"),
            Err(EvaluateError::TextCastFailed {
                literal: "abc".to_owned(),
                data_type: DataType::Int16
            }
            .into())
        );
        assert_eq!(
            cast_text_to_value(&DataType::Uint8, "255"),
            Ok(Value::U8(255))
        );
        assert_eq!(
            cast_text_to_value(&DataType::Uint8, "-1"),
            Err(EvaluateError::TextCastFailed {
                literal: "-1".to_owned(),
                data_type: DataType::Uint8
            }
            .into())
        );
        assert_eq!(
            cast_text_to_value(&DataType::Float32, "1.5"),
            Ok(Value::F32(1.5))
        );
        match cast_text_to_value(&DataType::Float32, "nan") {
            Ok(Value::F32(v)) if v.is_nan() => {}
            other => panic!("expected NaN, got {other:?}"),
        }
        assert_eq!(
            cast_text_to_value(&DataType::Decimal, "200"),
            Ok(Value::Decimal(Decimal::new(200, 0)))
        );
        assert_eq!(
            cast_text_to_value(&DataType::Decimal, "oops"),
            Err(EvaluateError::TextCastFailed {
                literal: "oops".to_owned(),
                data_type: DataType::Decimal
            }
            .into())
        );
        assert_eq!(
            cast_text_to_value(&DataType::Text, "hello"),
            Ok(Value::Str("hello".to_owned()))
        );
    }
}
