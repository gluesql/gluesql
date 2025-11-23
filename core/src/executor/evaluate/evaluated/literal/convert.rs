use {
    super::LiteralError,
    crate::{
        ast::DataType,
        data::{
            BigDecimalExt, Interval, Point, Value,
            value::{parse_date, parse_time, parse_timestamp, parse_uuid},
        },
        result::Result,
    },
    bigdecimal::BigDecimal,
    rust_decimal::Decimal,
    std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr},
        str::FromStr,
    },
};

pub(crate) fn number_literal_to_value(data_type: &DataType, value: &BigDecimal) -> Result<Value> {
    match data_type {
        DataType::Int8 => value
            .to_i8()
            .map(Value::I8)
            .ok_or_else(|| LiteralError::LiteralCastToInt8Failed(value.to_string()).into()),
        DataType::Int16 => value.to_i16().map(Value::I16).ok_or_else(|| {
            LiteralError::LiteralCastToDataTypeFailed(DataType::Int16, value.to_string()).into()
        }),
        DataType::Int32 => value.to_i32().map(Value::I32).ok_or_else(|| {
            LiteralError::LiteralCastToDataTypeFailed(DataType::Int32, value.to_string()).into()
        }),
        DataType::Int => value.to_i64().map(Value::I64).ok_or_else(|| {
            LiteralError::LiteralCastToDataTypeFailed(DataType::Int, value.to_string()).into()
        }),
        DataType::Int128 => value.to_i128().map(Value::I128).ok_or_else(|| {
            LiteralError::LiteralCastToDataTypeFailed(DataType::Int128, value.to_string()).into()
        }),
        DataType::Uint8 => value
            .to_u8()
            .map(Value::U8)
            .ok_or_else(|| LiteralError::LiteralCastToUnsignedInt8Failed(value.to_string()).into()),
        DataType::Uint16 => value
            .to_u16()
            .map(Value::U16)
            .ok_or_else(|| LiteralError::LiteralCastToUint16Failed(value.to_string()).into()),
        DataType::Uint32 => value
            .to_u32()
            .map(Value::U32)
            .ok_or_else(|| LiteralError::LiteralCastToUint32Failed(value.to_string()).into()),
        DataType::Uint64 => value
            .to_u64()
            .map(Value::U64)
            .ok_or_else(|| LiteralError::LiteralCastToUint64Failed(value.to_string()).into()),
        DataType::Uint128 => value
            .to_u128()
            .map(Value::U128)
            .ok_or_else(|| LiteralError::LiteralCastToUint128Failed(value.to_string()).into()),
        DataType::Float32 => value.to_f32().map(Value::F32).ok_or_else(|| {
            LiteralError::UnreachableLiteralCastFromNumberToFloat(value.to_string()).into()
        }),
        DataType::Float => value.to_f64().map(Value::F64).ok_or_else(|| {
            LiteralError::UnreachableLiteralCastFromNumberToFloat(value.to_string()).into()
        }),
        DataType::Inet => {
            if let Some(v4) = value.to_u32() {
                Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(v4))))
            } else {
                value
                    .to_u128()
                    .map(|v6| Value::Inet(IpAddr::V6(Ipv6Addr::from(v6))))
                    .ok_or_else(|| LiteralError::FailedToParseInetString(value.to_string()).into())
            }
        }
        DataType::Decimal => value
            .to_string()
            .parse::<Decimal>()
            .map(Value::Decimal)
            .map_err(|_| {
                LiteralError::LiteralCastFromTextToDecimalFailed(value.to_string()).into()
            }),
        _ => Err(LiteralError::IncompatibleLiteralForDataType {
            data_type: data_type.clone(),
            literal: value.to_string(),
        }
        .into()),
    }
}

pub(crate) fn cast_literal_number_to_value(
    data_type: &DataType,
    value: &BigDecimal,
) -> Result<Value> {
    match data_type {
        DataType::Boolean => {
            let literal = value.to_string();

            match value.to_i64() {
                Some(0) => Ok(Value::Bool(false)),
                Some(1) => Ok(Value::Bool(true)),
                _ => Err(LiteralError::LiteralCastToBooleanFailed(literal).into()),
            }
        }
        DataType::Text => Ok(Value::Str(value.to_string())),
        _ => number_literal_to_value(data_type, value),
    }
}

pub(crate) fn text_literal_to_value(data_type: &DataType, value: &str) -> Result<Value> {
    match data_type {
        DataType::Text => Ok(Value::Str(value.to_owned())),
        DataType::Bytea => hex::decode(value)
            .map(Value::Bytea)
            .map_err(|_| LiteralError::FailedToParseHexString(value.to_owned()).into()),
        DataType::Inet => IpAddr::from_str(value)
            .map(Value::Inet)
            .map_err(|_| LiteralError::FailedToParseInetString(value.to_owned()).into()),
        DataType::Interval => Interval::parse(value).map(Value::Interval),
        DataType::Point => Point::from_wkt(value)
            .map(Value::Point)
            .map_err(|_| LiteralError::FailedToParsePoint(value.to_owned()).into()),
        DataType::Date => parse_date(value)
            .map(Value::Date)
            .ok_or_else(|| LiteralError::LiteralCastToDateFailed(value.to_owned()).into()),
        DataType::Timestamp => parse_timestamp(value)
            .map(Value::Timestamp)
            .ok_or_else(|| LiteralError::LiteralCastToTimestampFailed(value.to_owned()).into()),
        DataType::Time => parse_time(value)
            .map(Value::Time)
            .ok_or_else(|| LiteralError::LiteralCastToTimeFailed(value.to_owned()).into()),
        DataType::Uuid => parse_uuid(value).map(Value::Uuid),
        DataType::Map => Value::parse_json_map(value),
        DataType::List => Value::parse_json_list(value),
        _ => Err(LiteralError::IncompatibleLiteralForDataType {
            data_type: data_type.clone(),
            literal: value.to_owned(),
        }
        .into()),
    }
}

pub(crate) fn cast_literal_text_to_value(data_type: &DataType, value: &str) -> Result<Value> {
    match data_type {
        DataType::Boolean => match value.to_uppercase().as_str() {
            "TRUE" | "1" => Ok(Value::Bool(true)),
            "FALSE" | "0" => Ok(Value::Bool(false)),
            _ => Err(LiteralError::LiteralCastToBooleanFailed(value.to_owned()).into()),
        },
        DataType::Int8 => value
            .parse::<i8>()
            .map(Value::I8)
            .map_err(|_| LiteralError::LiteralCastFromTextToIntegerFailed(value.to_owned()).into()),
        DataType::Int16 => value
            .parse::<i16>()
            .map(Value::I16)
            .map_err(|_| LiteralError::LiteralCastFromTextToIntegerFailed(value.to_owned()).into()),
        DataType::Int32 => value
            .parse::<i32>()
            .map(Value::I32)
            .map_err(|_| LiteralError::LiteralCastFromTextToIntegerFailed(value.to_owned()).into()),
        DataType::Int => value
            .parse::<i64>()
            .map(Value::I64)
            .map_err(|_| LiteralError::LiteralCastFromTextToIntegerFailed(value.to_owned()).into()),
        DataType::Int128 => value
            .parse::<i128>()
            .map(Value::I128)
            .map_err(|_| LiteralError::LiteralCastFromTextToIntegerFailed(value.to_owned()).into()),
        DataType::Uint8 => value.parse::<u8>().map(Value::U8).map_err(|_| {
            LiteralError::LiteralCastFromTextToUnsignedInt8Failed(value.to_owned()).into()
        }),
        DataType::Uint16 => value
            .parse::<u16>()
            .map(Value::U16)
            .map_err(|_| LiteralError::LiteralCastFromTextToUint16Failed(value.to_owned()).into()),
        DataType::Uint32 => value
            .parse::<u32>()
            .map(Value::U32)
            .map_err(|_| LiteralError::LiteralCastFromTextToUint32Failed(value.to_owned()).into()),
        DataType::Uint64 => value
            .parse::<u64>()
            .map(Value::U64)
            .map_err(|_| LiteralError::LiteralCastFromTextToUint64Failed(value.to_owned()).into()),
        DataType::Uint128 => value
            .parse::<u128>()
            .map(Value::U128)
            .map_err(|_| LiteralError::LiteralCastFromTextToUint128Failed(value.to_owned()).into()),
        DataType::Float32 => value
            .parse::<f32>()
            .map(Value::F32)
            .map_err(|_| LiteralError::LiteralCastFromTextToFloatFailed(value.to_owned()).into()),
        DataType::Float => value
            .parse::<f64>()
            .map(Value::F64)
            .map_err(|_| LiteralError::LiteralCastFromTextToFloatFailed(value.to_owned()).into()),
        DataType::Decimal => value
            .parse::<Decimal>()
            .map(Value::Decimal)
            .map_err(|_| LiteralError::LiteralCastFromTextToDecimalFailed(value.to_owned()).into()),
        _ => text_literal_to_value(data_type, value),
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{data::Value, executor::evaluate::Evaluated},
        bigdecimal::BigDecimal,
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        rust_decimal::Decimal,
        std::{borrow::Cow, net::IpAddr, str::FromStr},
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
                assert_eq!(super::parse_timestamp($timestamp), Some($result));
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
                assert_eq!(super::parse_time($time), Some($result));
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
    fn literal_to_value() {
        use crate::{ast::DataType, error::LiteralError, result::Result};

        fn literal_to_value(data_type: &DataType, evaluated: &Evaluated<'_>) -> Result<Value> {
            match evaluated {
                Evaluated::Number(value) => {
                    super::number_literal_to_value(data_type, value.as_ref())
                }
                Evaluated::Text(value) => super::text_literal_to_value(data_type, value.as_ref()),
                _ => unreachable!(),
            }
        }

        macro_rules! num {
            ($num: expr) => {
                Evaluated::Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }

        macro_rules! text {
            ($text: expr) => {
                Evaluated::Text(Cow::Owned($text.to_owned()))
            };
        }

        macro_rules! test {
            ($to: expr, $from: expr, $expected: expr) => {
                assert_eq!(literal_to_value(&$to, &$from), Ok($expected));
            };
        }

        let bytea = |v| hex::decode(v).unwrap();
        let inet = |v| IpAddr::from_str(v).unwrap();

        test!(DataType::Int, num!("123456789"), Value::I64(123_456_789));
        test!(DataType::Int8, num!("64"), Value::I8(64));
        test!(DataType::Int16, num!("64"), Value::I16(64));
        test!(DataType::Int32, num!("64"), Value::I32(64));
        test!(DataType::Int, num!("64"), Value::I64(64));
        test!(DataType::Int128, num!("64"), Value::I128(64));
        test!(DataType::Uint8, num!("8"), Value::U8(8));
        test!(DataType::Uint16, num!("64"), Value::U16(64));
        test!(DataType::Uint32, num!("64"), Value::U32(64));
        test!(DataType::Uint64, num!("64"), Value::U64(64));
        test!(DataType::Uint128, num!("64"), Value::U128(64));
        test!(
            DataType::Float32,
            num!("123456789"),
            Value::F32(123_456_789.0_f32)
        );
        test!(
            DataType::Float,
            num!("123456789"),
            Value::F64(123_456_789.0)
        );
        test!(
            DataType::Text,
            text!("Good!"),
            Value::Str("Good!".to_owned())
        );
        test!(DataType::Bytea, text!("1234"), Value::Bytea(bytea("1234")));
        assert_eq!(
            literal_to_value(&DataType::Bytea, &text!("123")),
            Err(LiteralError::FailedToParseHexString("123".to_owned()).into())
        );
        test!(DataType::Inet, text!("::1"), Value::Inet(inet("::1")));
        test!(
            DataType::Inet,
            num!("4294967295"),
            Value::Inet(inet("255.255.255.255"))
        );
        test!(
            DataType::Inet,
            num!("9876543210"),
            Value::Inet(inet("::2:4cb0:16ea"))
        );
        test!(
            DataType::Inet,
            num!("9876543210"),
            Value::Inet(inet("::2:4cb0:16ea"))
        );
        assert_eq!(
            literal_to_value(&DataType::Inet, &num!("1.5")),
            Err(LiteralError::FailedToParseInetString("1.5".to_owned()).into())
        );
        assert_eq!(
            literal_to_value(&DataType::Inet, &num!("-1")),
            Err(LiteralError::FailedToParseInetString("-1".to_owned()).into())
        );
        assert_eq!(
            literal_to_value(&DataType::Inet, &text!("123")),
            Err(LiteralError::FailedToParseInetString("123".to_owned()).into())
        );
        test!(
            DataType::Date,
            text!("2015-09-05"),
            Value::Date(NaiveDate::from_ymd_opt(2015, 9, 5).unwrap())
        );
        test!(
            DataType::Timestamp,
            text!("2022-12-20 10:00:00.987"),
            Value::Timestamp(date_time(2022, 12, 20, 10, 0, 0, 987))
        );
        test!(
            DataType::Time,
            text!("12:00:35"),
            Value::Time(chrono::NaiveTime::from_hms_milli_opt(12, 0, 35, 0).unwrap())
        );
        test!(
            DataType::Uuid,
            text!("936DA01F9ABD4d9d80C702AF85C822A8"),
            Value::Uuid(195_965_723_427_462_096_757_863_453_463_987_888_808)
        );

        assert_eq!(
            literal_to_value(
                &DataType::Map,
                &text!(
                    r#"{
            "name": "John Doe",
            "age": 43
        }"#
                )
            ),
            Value::parse_json_map(
                r#"{
            "name": "John Doe",
            "age": 43
        }"#
            )
        );
        assert_eq!(
            literal_to_value(
                &DataType::List,
                &text!(
                    r#"[
            "+44 1234567",
            "+44 2345678"
        ]"#
                )
            ),
            Value::parse_json_list(
                r#"[
            "+44 1234567",
            "+44 2345678"
        ]"#
            )
        );
        test!(
            DataType::Decimal,
            num!("200"),
            Value::Decimal(Decimal::new(200, 0))
        );
    }

    #[test]
    fn try_from() {
        macro_rules! text {
            ($text: expr) => {
                Evaluated::Text(Cow::Owned($text.to_owned()))
            };
        }

        macro_rules! num {
            ($num: expr) => {
                Evaluated::Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }

        macro_rules! test {
            ($from: expr, $expected: expr) => {
                assert_eq!(
                    utils::Tribool::True,
                    Value::try_from($from).unwrap().evaluate_eq(&$expected)
                );
            };
        }

        test!(text!("hello"), Value::Str("hello".to_owned()));
        test!(text!("hallo"), Value::Str("hallo".to_owned()));
        test!(num!("1234567890"), Value::I64(1_234_567_890));
        test!(num!("1.0"), Value::F32(1.0_f32));
        test!(num!("1.0"), Value::F64(1.0));
    }
}
