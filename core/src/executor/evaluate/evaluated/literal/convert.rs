use {
    super::{Literal, LiteralError},
    crate::{
        ast::DataType,
        data::{
            BigDecimalExt, Interval, Point, Value, ValueError,
            value::{parse_date, parse_time, parse_timestamp, parse_uuid},
        },
        result::{Error, Result},
    },
    bigdecimal::BigDecimal,
    rust_decimal::Decimal,
    std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr},
        str::FromStr,
    },
};

impl TryFrom<&Literal<'_>> for Value {
    type Error = Error;

    fn try_from(literal: &Literal<'_>) -> Result<Self> {
        match literal {
            Literal::Number(v) => v
                .to_i64()
                .map(Value::I64)
                .or_else(|| v.to_f64().map(Value::F64))
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            Literal::Text(v) => Ok(Value::Str(v.as_ref().to_owned())),
        }
    }
}

impl TryFrom<Literal<'_>> for Value {
    type Error = Error;

    fn try_from(literal: Literal<'_>) -> Result<Self> {
        match literal {
            Literal::Text(v) => Ok(Value::Str(v.into_owned())),
            number @ Literal::Number(_) => Value::try_from(&number),
        }
    }
}

pub(crate) fn literal_to_value(data_type: &DataType, literal: &Literal<'_>) -> Result<Value> {
    let result = match literal {
        Literal::Number(value) => literal_number_to_value(data_type, value.as_ref()),
        Literal::Text(value) => literal_text_to_value(data_type, value.as_ref()),
    };

    match result {
        Some(output) => output,
        None => Err(LiteralError::IncompatibleLiteralForDataType {
            data_type: data_type.clone(),
            literal: literal.to_string(),
        }
        .into()),
    }
}

fn literal_number_to_value(data_type: &DataType, value: &BigDecimal) -> Option<Result<Value>> {
    match data_type {
        DataType::Int8 => Some(
            value
                .to_i8()
                .map(Value::I8)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
        ),
        DataType::Int16 => Some(
            value
                .to_i16()
                .map(Value::I16)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
        ),
        DataType::Int32 => Some(
            value
                .to_i32()
                .map(Value::I32)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
        ),
        DataType::Int => Some(
            value
                .to_i64()
                .map(Value::I64)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
        ),
        DataType::Int128 => Some(
            value
                .to_i128()
                .map(Value::I128)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
        ),
        DataType::Uint8 => Some(
            value
                .to_u8()
                .map(Value::U8)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
        ),
        DataType::Uint16 => Some(
            value
                .to_u16()
                .map(Value::U16)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
        ),
        DataType::Uint32 => Some(
            value
                .to_u32()
                .map(Value::U32)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
        ),
        DataType::Uint64 => Some(
            value
                .to_u64()
                .map(Value::U64)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
        ),
        DataType::Uint128 => Some(
            value
                .to_u128()
                .map(Value::U128)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
        ),
        DataType::Float32 => Some(
            value
                .to_f32()
                .map(Value::F32)
                .ok_or_else(|| ValueError::UnreachableNumberParsing.into()),
        ),
        DataType::Float => Some(
            value
                .to_f64()
                .map(Value::F64)
                .ok_or_else(|| ValueError::UnreachableNumberParsing.into()),
        ),
        DataType::Inet => Some(Ok(if let Some(v4) = value.to_u32() {
            Value::Inet(IpAddr::V4(Ipv4Addr::from(v4)))
        } else {
            Value::Inet(IpAddr::V6(Ipv6Addr::from(value.to_u128().unwrap())))
        })),
        DataType::Decimal => Some(
            value
                .to_string()
                .parse::<Decimal>()
                .map(Value::Decimal)
                .map_err(|_| ValueError::FailedToParseDecimal(value.to_string()).into()),
        ),
        _ => None,
    }
}

fn literal_text_to_value(data_type: &DataType, value: &str) -> Option<Result<Value>> {
    match data_type {
        DataType::Text => Some(Ok(Value::Str(value.to_owned()))),
        DataType::Bytea => Some(
            hex::decode(value)
                .map(Value::Bytea)
                .map_err(|_| ValueError::FailedToParseHexString(value.to_owned()).into()),
        ),
        DataType::Inet => Some(
            IpAddr::from_str(value)
                .map(Value::Inet)
                .map_err(|_| ValueError::FailedToParseInetString(value.to_owned()).into()),
        ),
        DataType::Date => Some(
            value
                .parse::<chrono::NaiveDate>()
                .map(Value::Date)
                .map_err(|_| ValueError::FailedToParseDate(value.to_owned()).into()),
        ),
        DataType::Timestamp => Some(
            parse_timestamp(value)
                .map(Value::Timestamp)
                .ok_or_else(|| ValueError::FailedToParseTimestamp(value.to_owned()).into()),
        ),
        DataType::Time => Some(
            parse_time(value)
                .map(Value::Time)
                .ok_or_else(|| ValueError::FailedToParseTime(value.to_owned()).into()),
        ),
        DataType::Uuid => Some(parse_uuid(value).map(Value::Uuid)),
        DataType::Map => Some(Value::parse_json_map(value)),
        DataType::List => Some(Value::parse_json_list(value)),
        _ => None,
    }
}

pub(crate) fn try_cast_literal_to_value(
    data_type: &DataType,
    literal: &Literal<'_>,
) -> Result<Value> {
    let result = match literal {
        Literal::Number(value) => cast_literal_number_to_value(data_type, value.as_ref()),
        Literal::Text(value) => cast_literal_text_to_value(data_type, value.as_ref()),
    };

    match result {
        Some(output) => output,
        None => match literal_to_value(data_type, literal) {
            Ok(value) => Ok(value),
            Err(error) => map_cast_error(data_type, literal, error),
        },
    }
}

fn cast_literal_number_to_value(data_type: &DataType, value: &BigDecimal) -> Option<Result<Value>> {
    match data_type {
        DataType::Boolean => Some(match value.to_i64() {
            Some(0) => Ok(Value::Bool(false)),
            Some(1) => Ok(Value::Bool(true)),
            _ => Err(ValueError::LiteralCastToBooleanFailed(value.to_string()).into()),
        }),
        DataType::Text => Some(Ok(Value::Str(value.to_string()))),
        DataType::Inet => Some(Ok(if let Some(v4) = value.to_u32() {
            Value::Inet(IpAddr::V4(Ipv4Addr::from(v4)))
        } else {
            Value::Inet(IpAddr::V6(Ipv6Addr::from(value.to_u128().unwrap())))
        })),
        _ => None,
    }
}

fn cast_literal_text_to_value(data_type: &DataType, value: &str) -> Option<Result<Value>> {
    match data_type {
        DataType::Boolean => Some(match value.to_uppercase().as_str() {
            "TRUE" | "1" => Ok(Value::Bool(true)),
            "FALSE" | "0" => Ok(Value::Bool(false)),
            _ => Err(ValueError::LiteralCastToBooleanFailed(value.to_owned()).into()),
        }),
        DataType::Int8 => Some(cast_text_with::<i8, _, _>(
            value,
            Value::I8,
            ValueError::LiteralCastFromTextToIntegerFailed,
        )),
        DataType::Int16 => Some(cast_text_with::<i16, _, _>(
            value,
            Value::I16,
            ValueError::LiteralCastFromTextToIntegerFailed,
        )),
        DataType::Int32 => Some(cast_text_with::<i32, _, _>(
            value,
            Value::I32,
            ValueError::LiteralCastFromTextToIntegerFailed,
        )),
        DataType::Int => Some(cast_text_with::<i64, _, _>(
            value,
            Value::I64,
            ValueError::LiteralCastFromTextToIntegerFailed,
        )),
        DataType::Int128 => Some(cast_text_with::<i128, _, _>(
            value,
            Value::I128,
            ValueError::LiteralCastFromTextToIntegerFailed,
        )),
        DataType::Uint8 => Some(cast_text_with::<u8, _, _>(
            value,
            Value::U8,
            ValueError::LiteralCastFromTextToUnsignedInt8Failed,
        )),
        DataType::Uint16 => Some(cast_text_with::<u16, _, _>(
            value,
            Value::U16,
            ValueError::LiteralCastFromTextToUint16Failed,
        )),
        DataType::Uint32 => Some(cast_text_with::<u32, _, _>(
            value,
            Value::U32,
            ValueError::LiteralCastFromTextToUint32Failed,
        )),
        DataType::Uint64 => Some(cast_text_with::<u64, _, _>(
            value,
            Value::U64,
            ValueError::LiteralCastFromTextToUint64Failed,
        )),
        DataType::Uint128 => Some(cast_text_with::<u128, _, _>(
            value,
            Value::U128,
            ValueError::LiteralCastFromTextToUint128Failed,
        )),
        DataType::Float32 => Some(cast_text_with::<f32, _, _>(
            value,
            Value::F32,
            ValueError::LiteralCastFromTextToFloatFailed,
        )),
        DataType::Float => Some(cast_text_with::<f64, _, _>(
            value,
            Value::F64,
            ValueError::LiteralCastFromTextToFloatFailed,
        )),
        DataType::Decimal => Some(cast_text_with::<Decimal, _, _>(
            value,
            Value::Decimal,
            ValueError::LiteralCastFromTextToDecimalFailed,
        )),
        DataType::Interval => Some(Interval::parse(value).map(Value::Interval)),
        DataType::Point => Some(
            Point::from_wkt(value)
                .map(Value::Point)
                .map_err(|_| ValueError::FailedToParsePoint(value.to_owned()).into()),
        ),
        DataType::Date => Some(
            parse_date(value)
                .map(Value::Date)
                .ok_or_else(|| ValueError::LiteralCastToDateFailed(value.to_owned()).into()),
        ),
        _ => None,
    }
}

fn cast_text_with<T, Wrap, ErrFn>(text: &str, wrap: Wrap, err: ErrFn) -> Result<Value>
where
    T: FromStr,
    Wrap: Fn(T) -> Value,
    ErrFn: Fn(String) -> ValueError,
{
    text.parse::<T>()
        .map(wrap)
        .map_err(|_| err(text.to_owned()).into())
}

fn map_cast_error(data_type: &DataType, literal: &Literal<'_>, error: Error) -> Result<Value> {
    let Error::Value(value_error) = &error else {
        return Err(error);
    };

    match literal {
        Literal::Number(number) => {
            let literal_string = number.to_string();
            let mapped_error = match (data_type, &**value_error) {
                (DataType::Int8 | DataType::Int16, ValueError::FailedToParseNumber) => {
                    Some(ValueError::LiteralCastToInt8Failed(literal_string))
                }
                (DataType::Int32, ValueError::FailedToParseNumber) => Some(
                    ValueError::LiteralCastToDataTypeFailed(DataType::Int32, literal_string),
                ),
                (DataType::Int, ValueError::FailedToParseNumber) => Some(
                    ValueError::LiteralCastToDataTypeFailed(DataType::Int, literal_string),
                ),
                (DataType::Int128, ValueError::FailedToParseNumber) => Some(
                    ValueError::LiteralCastToDataTypeFailed(DataType::Int128, literal_string),
                ),
                (DataType::Uint8, ValueError::FailedToParseNumber) => {
                    Some(ValueError::LiteralCastToUnsignedInt8Failed(literal_string))
                }
                (DataType::Uint16, ValueError::FailedToParseNumber) => {
                    Some(ValueError::LiteralCastToUint16Failed(literal_string))
                }
                (DataType::Uint32, ValueError::FailedToParseNumber) => {
                    Some(ValueError::LiteralCastToUint32Failed(literal_string))
                }
                (DataType::Uint64, ValueError::FailedToParseNumber) => {
                    Some(ValueError::LiteralCastToUint64Failed(literal_string))
                }
                (DataType::Uint128, ValueError::FailedToParseNumber) => {
                    Some(ValueError::LiteralCastToUint128Failed(literal_string))
                }
                (DataType::Float32 | DataType::Float, ValueError::UnreachableNumberParsing) => {
                    Some(ValueError::UnreachableLiteralCastFromNumberToFloat(
                        literal_string,
                    ))
                }
                (DataType::Decimal, ValueError::FailedToParseDecimal(_)) => Some(
                    ValueError::LiteralCastFromTextToDecimalFailed(literal_string),
                ),
                _ => None,
            };

            match mapped_error {
                Some(mapped) => Err(mapped.into()),
                None => Err(error),
            }
        }
        Literal::Text(value) => {
            let literal_string = value.to_string();
            let mapped_error = match (data_type, &**value_error) {
                (DataType::Time, ValueError::FailedToParseTime(_)) => {
                    Some(ValueError::LiteralCastToTimeFailed(literal_string))
                }
                (DataType::Timestamp, ValueError::FailedToParseTimestamp(_)) => {
                    Some(ValueError::LiteralCastToTimestampFailed(literal_string))
                }
                _ => None,
            };

            match mapped_error {
                Some(mapped) => Err(mapped.into()),
                None => Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Literal,
        crate::data::Value,
        bigdecimal::BigDecimal,
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        rust_decimal::Decimal,
        std::{
            borrow::Cow,
            net::{IpAddr, Ipv4Addr, Ipv6Addr},
            str::FromStr,
        },
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
        use crate::{ast::DataType, data::ValueError};

        macro_rules! num {
            ($num: expr) => {
                Literal::Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }

        macro_rules! text {
            ($text: expr) => {
                Literal::Text(Cow::Owned($text.to_owned()))
            };
        }

        macro_rules! test {
            ($to: expr, $from: expr, $expected: expr) => {
                assert_eq!(super::literal_to_value(&$to, &$from), Ok($expected));
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
            super::literal_to_value(&DataType::Bytea, &text!("123")),
            Err(ValueError::FailedToParseHexString("123".to_owned()).into())
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
            super::literal_to_value(&DataType::Inet, &text!("123")),
            Err(ValueError::FailedToParseInetString("123".to_owned()).into())
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
            super::literal_to_value(
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
            super::literal_to_value(
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
        use {
            super::Literal,
            crate::data::Value,
            bigdecimal::BigDecimal,
            std::{borrow::Cow, str::FromStr},
        };

        macro_rules! text {
            ($text: expr) => {
                Literal::Text(Cow::Owned($text.to_owned()))
            };
        }

        macro_rules! num {
            ($num: expr) => {
                &Literal::Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
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
        test!(&text!("hallo"), Value::Str("hallo".to_owned()));
        test!(num!("1234567890"), Value::I64(1_234_567_890));
        test!(num!("1.0"), Value::F32(1.0_f32));
        test!(num!("1.0"), Value::F64(1.0));
    }

    #[test]
    fn try_cast_literal_to_value() {
        use crate::{ast::DataType, data::Interval as I};

        macro_rules! text {
            ($text: expr) => {
                Literal::Text(Cow::Owned($text.to_owned()))
            };
        }

        macro_rules! num {
            ($num: expr) => {
                &Literal::Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }

        macro_rules! test {
            ($to: expr, $from: expr, $expected: expr) => {
                let actual = super::try_cast_literal_to_value(&$to, &$from);

                assert_eq!(actual, Ok($expected))
            };
        }

        let timestamp = |y, m, d, hh, mm, ss, ms| {
            NaiveDate::from_ymd_opt(y, m, d)
                .unwrap()
                .and_hms_milli_opt(hh, mm, ss, ms)
                .unwrap()
        };

        test!(DataType::Boolean, text!("false"), Value::Bool(false));
        test!(DataType::Boolean, text!("true"), Value::Bool(true));
        test!(DataType::Boolean, num!("0"), Value::Bool(false));
        test!(DataType::Boolean, num!("1"), Value::Bool(true));

        test!(DataType::Int8, text!("127"), Value::I8(127));
        test!(DataType::Int8, num!("125"), Value::I8(125));

        test!(DataType::Int16, text!("127"), Value::I16(127));
        test!(DataType::Int16, num!("125"), Value::I16(125));

        test!(DataType::Int32, text!("127"), Value::I32(127));
        test!(DataType::Int32, num!("125"), Value::I32(125));

        test!(
            DataType::Int,
            text!("1234567890"),
            Value::I64(1_234_567_890)
        );
        test!(DataType::Int, num!("1234567890"), Value::I64(1_234_567_890));
        test!(DataType::Int128, text!("127"), Value::I128(127));
        test!(DataType::Int128, num!("125"), Value::I128(125));

        test!(DataType::Uint8, text!("127"), Value::U8(127));
        test!(DataType::Uint8, num!("125"), Value::U8(125));

        test!(DataType::Uint16, text!("127"), Value::U16(127));
        test!(DataType::Uint16, num!("125"), Value::U16(125));

        test!(DataType::Uint32, text!("127"), Value::U32(127));
        test!(DataType::Uint32, num!("125"), Value::U32(125));

        test!(DataType::Uint64, text!("127"), Value::U64(127));
        test!(DataType::Uint64, num!("125"), Value::U64(125));

        test!(DataType::Uint128, text!("127"), Value::U128(127));
        test!(DataType::Uint128, num!("125"), Value::U128(125));

        test!(
            DataType::Float32,
            text!("12345.67"),
            Value::F32(12345.67_f32)
        );
        test!(
            DataType::Float32,
            num!("123456.78"),
            Value::F32(123_456.78_f32)
        );

        test!(DataType::Float, text!("12345.6789"), Value::F64(12345.6789));
        test!(DataType::Float, num!("123456.789"), Value::F64(123_456.789));
        test!(
            DataType::Text,
            num!("1234567890"),
            Value::Str("1234567890".to_owned())
        );
        test!(DataType::Text, text!("Cow"), Value::Str("Cow".to_owned()));
        test!(
            DataType::Interval,
            text!("'+22-10' YEAR TO MONTH"),
            Value::Interval(I::Month(274))
        );
        test!(
            DataType::Uuid,
            text!("936DA01F9ABD4d9d80C702AF85C822A8"),
            Value::Uuid(195_965_723_427_462_096_757_863_453_463_987_888_808)
        );
        test!(
            DataType::Date,
            text!("2015-09-05"),
            Value::Date(NaiveDate::from_ymd_opt(2015, 9, 5).unwrap())
        );
        test!(
            DataType::Time,
            text!("12:00:35"),
            Value::Time(chrono::NaiveTime::from_hms_milli_opt(12, 0, 35, 0).unwrap())
        );
        test!(
            DataType::Timestamp,
            text!("2022-12-20 10:00:00.987"),
            Value::Timestamp(timestamp(2022, 12, 20, 10, 0, 0, 987))
        );
        test!(
            DataType::Inet,
            num!("1234567890"),
            Value::Inet(IpAddr::from(Ipv4Addr::from(1_234_567_890)))
        );
        test!(
            DataType::Inet,
            num!("91234567890"),
            Value::Inet(IpAddr::from(Ipv6Addr::from(91_234_567_890)))
        );
        test!(
            DataType::Inet,
            text!("::1"),
            Value::Inet(IpAddr::from_str("::1").unwrap())
        );
        test!(
            DataType::Map,
            text!(r#"{ "a": 1 }"#),
            Value::parse_json_map(r#"{ "a": 1 }"#).unwrap()
        );
        test!(
            DataType::List,
            text!(r"[ 1, 2, 3 ]"),
            Value::parse_json_list(r"[ 1, 2, 3 ]").unwrap()
        );
    }
}
