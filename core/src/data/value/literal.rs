use {
    super::{
        date::{parse_date, parse_time, parse_timestamp},
        error::ValueError,
        Value,
    },
    crate::{
        ast::DataType,
        data::{value::uuid::parse_uuid, BigDecimalExt, Interval, Literal},
        result::{Error, Result},
    },
    chrono::NaiveDate,
    rust_decimal::Decimal,
    std::{
        cmp::Ordering,
        net::{IpAddr, Ipv4Addr, Ipv6Addr},
        str::FromStr,
    },
};

impl PartialEq<Literal<'_>> for Value {
    fn eq(&self, other: &Literal<'_>) -> bool {
        match (self, other) {
            (Value::Bool(l), Literal::Boolean(r)) => l == r,
            (Value::I8(l), Literal::Number(r)) => r.to_i8().map(|r| *l == r).unwrap_or(false),
            (Value::I16(l), Literal::Number(r)) => r.to_i16().map(|r| *l == r).unwrap_or(false),
            (Value::I32(l), Literal::Number(r)) => r.to_i32().map(|r| *l == r).unwrap_or(false),
            (Value::I64(l), Literal::Number(r)) => r.to_i64().map(|r| *l == r).unwrap_or(false),
            (Value::I128(l), Literal::Number(r)) => r.to_i128().map(|r| *l == r).unwrap_or(false),
            (Value::U8(l), Literal::Number(r)) => r.to_u8().map(|r| *l == r).unwrap_or(false),
            (Value::U16(l), Literal::Number(r)) => r.to_u16().map(|r| *l == r).unwrap_or(false),
            (Value::F64(l), Literal::Number(r)) => r.to_f64().map(|r| *l == r).unwrap_or(false),
            (Value::Str(l), Literal::Text(r)) => l == r.as_ref(),
            (Value::Bytea(l), Literal::Bytea(r)) => l == r,
            (Value::Date(l), Literal::Text(r)) => match r.parse::<NaiveDate>() {
                Ok(r) => l == &r,
                Err(_) => false,
            },
            (Value::Timestamp(l), Literal::Text(r)) => match parse_timestamp(r) {
                Some(r) => l == &r,
                None => false,
            },
            (Value::Time(l), Literal::Text(r)) => match parse_time(r) {
                Some(r) => l == &r,
                None => false,
            },
            (Value::Uuid(l), Literal::Text(r)) => parse_uuid(r).map(|r| l == &r).unwrap_or(false),
            (Value::Inet(l), Literal::Text(r)) => match IpAddr::from_str(r) {
                Ok(x) => l == &x,
                Err(_) => false,
            },
            (Value::Inet(l), Literal::Number(r)) => {
                if let Some(x) = r.to_u32() {
                    l == &Ipv4Addr::from(x)
                } else if let Some(x) = r.to_u128() {
                    l == &Ipv6Addr::from(x)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

impl PartialOrd<Literal<'_>> for Value {
    fn partial_cmp(&self, other: &Literal<'_>) -> Option<Ordering> {
        match (self, other) {
            (Value::I8(l), Literal::Number(r)) => {
                r.to_i8().map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            (Value::I16(l), Literal::Number(r)) => {
                r.to_i16().map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            (Value::I32(l), Literal::Number(r)) => {
                r.to_i32().map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            (Value::I64(l), Literal::Number(r)) => {
                r.to_i64().map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            (Value::I128(l), Literal::Number(r)) => {
                r.to_i128().map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            (Value::U8(l), Literal::Number(r)) => {
                r.to_u8().map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            (Value::U16(l), Literal::Number(r)) => {
                r.to_u16().map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            (Value::F64(l), Literal::Number(r)) => {
                r.to_f64().map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            (Value::Str(l), Literal::Text(r)) => {
                let l: &str = l.as_ref();
                Some(l.cmp(r))
            }
            (Value::Date(l), Literal::Text(r)) => match r.parse::<NaiveDate>() {
                Ok(r) => l.partial_cmp(&r),
                Err(_) => None,
            },
            (Value::Timestamp(l), Literal::Text(r)) => match parse_timestamp(r) {
                Some(r) => l.partial_cmp(&r),
                None => None,
            },
            (Value::Time(l), Literal::Text(r)) => match parse_time(r) {
                Some(r) => l.partial_cmp(&r),
                None => None,
            },
            (Value::Uuid(l), Literal::Text(r)) => {
                parse_uuid(r).map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            (Value::Inet(l), Literal::Text(r)) => match IpAddr::from_str(r) {
                Ok(x) => l.partial_cmp(&x),
                Err(_) => None,
            },
            (Value::Inet(l), Literal::Number(r)) => {
                if let Some(x) = r.to_u32() {
                    l.partial_cmp(&Ipv4Addr::from(x))
                } else if let Some(x) = r.to_u128() {
                    l.partial_cmp(&Ipv6Addr::from(x))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl TryFrom<&Literal<'_>> for Value {
    type Error = Error;

    fn try_from(literal: &Literal<'_>) -> Result<Self> {
        match literal {
            Literal::Number(v) => {
                if let Some(n) = v.to_i64() {
                    return Ok(Value::I64(n));
                } else if let Some(n) = v.to_f64() {
                    return Ok(Value::F64(n));
                }
                Err(ValueError::FailedToParseNumber.into())
            }
            Literal::Boolean(v) => Ok(Value::Bool(*v)),
            Literal::Text(v) => Ok(Value::Str(v.as_ref().to_owned())),
            Literal::Bytea(v) => Ok(Value::Bytea(v.to_vec())),
            Literal::Null => Ok(Value::Null),
        }
    }
}

impl TryFrom<Literal<'_>> for Value {
    type Error = Error;

    fn try_from(literal: Literal<'_>) -> Result<Self> {
        match literal {
            Literal::Text(v) => Ok(Value::Str(v.into_owned())),
            _ => Value::try_from(&literal),
        }
    }
}

impl Value {
    pub fn try_from_literal(data_type: &DataType, literal: &Literal<'_>) -> Result<Value> {
        match literal {
            Literal::Number(v) => {
                let value = match data_type {
                    DataType::Int8 => v.to_i8().map(Value::I8),
                    DataType::Int16 => v.to_i16().map(Value::I16),
                    DataType::Int32 => v.to_i32().map(Value::I32),
                    DataType::Int => v.to_i64().map(Value::I64),
                    DataType::Int128 => v.to_i128().map(Value::I128),
                    DataType::Uint8 => v.to_u8().map(Value::U8),
                    DataType::Uint16 => v.to_u16().map(Value::U16),
                    DataType::Float => v.to_f64().map(Value::F64),
                    DataType::Inet => {
                        if let Some(x) = v.to_u32() {
                            Some(Value::Inet(IpAddr::V4(Ipv4Addr::from(x))))
                        } else {
                            v.to_u128()
                                .map(|x| Value::Inet(IpAddr::V6(Ipv6Addr::from(x))))
                        }
                    }
                    DataType::Decimal => {
                        let Ok(value) = v.to_string().parse::<Decimal>() else {
                            return Err(ValueError::FailedToParseDecimal(v.to_string()).into());
                        };
                        Some(Value::Decimal(value))
                    }
                    _ => None,
                };
                return value.ok_or_else(|| ValueError::FailedToParseNumber.into());
            }
            Literal::Text(t) => {
                let value = match data_type {
                    DataType::Bytea => {
                        let Ok(value) = hex::decode(t.as_ref()) else {
                            return Err(ValueError::FailedToParseHexString(t.to_string()).into());
                        };
                        Some(Value::Bytea(value))
                    }
                    DataType::Inet => {
                        let Ok(value) = IpAddr::from_str(t.as_ref()) else {
                            return Err(ValueError::FailedToParseInetString(t.to_string()).into());
                        };
                        Some(Value::Inet(value))
                    }
                    DataType::Date => {
                        let Ok(value) = t.parse::<NaiveDate>() else {
                            return Err(ValueError::FailedToParseDate(t.to_string()).into());
                        };
                        Some(Value::Date(value))
                    }
                    DataType::Timestamp => {
                        let Some(value) = parse_timestamp(t) else {
                            return Err(ValueError::FailedToParseTimestamp(t.to_string()).into());
                        };
                        Some(Value::Timestamp(value))
                    }
                    DataType::Text => Some(Value::Str(t.to_string())),
                    DataType::Time => {
                        let Some(value) = parse_time(t) else {
                            return Err(ValueError::FailedToParseTime(t.to_string()).into());
                        };
                        Some(Value::Time(value))
                    }
                    DataType::Uuid => {
                        let value = parse_uuid(t)?;
                        Some(Value::Uuid(value))
                    }
                    DataType::Map => Some(Value::parse_json_map(t)?),
                    DataType::List => Some(Value::parse_json_list(t)?),
                    _ => None,
                };
                if let Some(value) = value {
                    return Ok(value);
                }
            }
            Literal::Boolean(b) => {
                if matches!(data_type, DataType::Boolean) {
                    return Ok(Value::Bool(*b));
                }
            }
            Literal::Bytea(b) => {
                if matches!(data_type, DataType::Bytea) {
                    return Ok(Value::Bytea(b.to_vec()));
                }
            }
            Literal::Null => {
                return Ok(Value::Null);
            }
        }
        Err(ValueError::IncompatibleLiteralForDataType {
            data_type: data_type.clone(),
            literal: format!("{literal:?}"),
        }
        .into())
    }

    pub fn try_cast_from_literal(data_type: &DataType, literal: &Literal<'_>) -> Result<Value> {
        match (data_type, literal) {
            (DataType::Boolean, Literal::Boolean(v)) => Ok(Value::Bool(*v)),
            (DataType::Boolean, Literal::Text(v)) => match v.to_uppercase().as_str() {
                "TRUE" | "1" => Ok(Value::Bool(true)),
                "FALSE" | "0" => Ok(Value::Bool(false)),
                _ => Err(ValueError::LiteralCastToBooleanFailed(v.to_string()).into()),
            },
            (DataType::Boolean, Literal::Number(v)) => match v.to_i64() {
                Some(0) => Ok(Value::Bool(false)),
                Some(1) => Ok(Value::Bool(true)),
                _ => Err(ValueError::LiteralCastToBooleanFailed(v.to_string()).into()),
            },
            (DataType::Int8, Literal::Text(v)) => v
                .parse::<i8>()
                .map(Value::I8)
                .map_err(|_| ValueError::LiteralCastFromTextToIntegerFailed(v.to_string()).into()),
            (DataType::Int8, Literal::Number(v)) => match v.to_i8() {
                Some(x) => Ok(Value::I8(x)),
                None => Err(ValueError::LiteralCastToInt8Failed(v.to_string()).into()),
            },
            (DataType::Int8, Literal::Boolean(v)) => {
                let v = i8::from(*v);

                Ok(Value::I8(v))
            }
            (DataType::Int16, Literal::Text(v)) => v
                .parse::<i16>()
                .map(Value::I16)
                .map_err(|_| ValueError::LiteralCastFromTextToIntegerFailed(v.to_string()).into()),
            (DataType::Int16, Literal::Number(v)) => match v.to_i16() {
                Some(x) => Ok(Value::I16(x)),
                None => Err(ValueError::LiteralCastToInt8Failed(v.to_string()).into()),
            },
            (DataType::Int16, Literal::Boolean(v)) => {
                let v = i16::from(*v);

                Ok(Value::I16(v))
            }
            (DataType::Int32, Literal::Text(v)) => v
                .parse::<i32>()
                .map(Value::I32)
                .map_err(|_| ValueError::LiteralCastFromTextToIntegerFailed(v.to_string()).into()),
            (DataType::Int32, Literal::Number(v)) => match v.to_i32() {
                Some(x) => Ok(Value::I32(x)),
                None => Err(ValueError::LiteralCastToDataTypeFailed(
                    DataType::Int32,
                    v.to_string(),
                )
                .into()),
            },
            (DataType::Int32, Literal::Boolean(v)) => {
                let v = i32::from(*v);

                Ok(Value::I32(v))
            }
            (DataType::Int, Literal::Text(v)) => v
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| ValueError::LiteralCastFromTextToIntegerFailed(v.to_string()).into()),
            (DataType::Int, Literal::Number(v)) => match v.to_i64() {
                Some(x) => Ok(Value::I64(x)),
                None => Err(
                    ValueError::LiteralCastToDataTypeFailed(DataType::Int, v.to_string()).into(),
                ),
            },
            (DataType::Int, Literal::Boolean(v)) => {
                let v = i64::from(*v);

                Ok(Value::I64(v))
            }
            (DataType::Int128, Literal::Text(v)) => v
                .parse::<i128>()
                .map(Value::I128)
                .map_err(|_| ValueError::LiteralCastFromTextToIntegerFailed(v.to_string()).into()),
            (DataType::Int128, Literal::Number(v)) => match v.to_i128() {
                Some(x) => Ok(Value::I128(x)),
                None => Err(ValueError::LiteralCastToDataTypeFailed(
                    DataType::Int128,
                    v.to_string(),
                )
                .into()),
            },
            (DataType::Int128, Literal::Boolean(v)) => {
                let v = i128::from(*v);

                Ok(Value::I128(v))
            }
            (DataType::Uint8, Literal::Text(v)) => v.parse::<u8>().map(Value::U8).map_err(|_| {
                ValueError::LiteralCastFromTextToUnsignedInt8Failed(v.to_string()).into()
            }),
            (DataType::Uint8, Literal::Number(v)) => match v.to_u8() {
                Some(x) => Ok(Value::U8(x)),
                None => Err(ValueError::LiteralCastToUnsignedInt8Failed(v.to_string()).into()),
            },
            (DataType::Uint8, Literal::Boolean(v)) => {
                let v = u8::from(*v);

                Ok(Value::U8(v))
            }
            (DataType::Uint16, Literal::Text(v)) => v
                .parse::<u16>()
                .map(Value::U16)
                .map_err(|_| ValueError::LiteralCastFromTextToUint16Failed(v.to_string()).into()),
            (DataType::Uint16, Literal::Number(v)) => match v.to_u16() {
                Some(x) => Ok(Value::U16(x)),
                None => Err(ValueError::LiteralCastToUint16Failed(v.to_string()).into()),
            },
            (DataType::Uint16, Literal::Boolean(v)) => {
                let v = u16::from(*v);

                Ok(Value::U16(v))
            }
            (DataType::Float, Literal::Text(v)) => v
                .parse::<f64>()
                .map(Value::F64)
                .map_err(|_| ValueError::LiteralCastFromTextToFloatFailed(v.to_string()).into()),
            (DataType::Float, Literal::Number(v)) => v.to_f64().map(Value::F64).ok_or_else(|| {
                ValueError::UnreachableLiteralCastFromNumberToFloat(v.to_string()).into()
            }),
            (DataType::Float, Literal::Boolean(v)) => {
                let v = if *v { 1.0 } else { 0.0 };

                Ok(Value::F64(v))
            }
            (DataType::Decimal, Literal::Text(v)) => v
                .parse::<Decimal>()
                .map(Value::Decimal)
                .map_err(|_| ValueError::LiteralCastFromTextToDecimalFailed(v.to_string()).into()),
            (DataType::Decimal, Literal::Number(v)) => v
                .to_string()
                .parse::<Decimal>()
                .map(Value::Decimal)
                .map_err(|_| ValueError::LiteralCastFromTextToDecimalFailed(v.to_string()).into()),
            (DataType::Decimal, Literal::Boolean(v)) => {
                let v = if *v { Decimal::ONE } else { Decimal::ZERO };

                Ok(Value::Decimal(v))
            }

            (DataType::Text, Literal::Number(v)) => Ok(Value::Str(v.to_string())),
            (DataType::Text, Literal::Text(v)) => Ok(Value::Str(v.to_string())),
            (DataType::Text, Literal::Boolean(v)) => {
                let v = if *v { "TRUE" } else { "FALSE" };

                Ok(Value::Str(v.to_owned()))
            }
            (DataType::Interval, Literal::Text(v)) => {
                Interval::try_from(v.as_ref()).map(Value::Interval)
            }
            (DataType::Uuid, Literal::Text(v)) => parse_uuid(v).map(Value::Uuid),
            (DataType::Boolean, Literal::Null)
            | (DataType::Int8, Literal::Null)
            | (DataType::Int16, Literal::Null)
            | (DataType::Int32, Literal::Null)
            | (DataType::Int, Literal::Null)
            | (DataType::Int128, Literal::Null)
            | (DataType::Uint8, Literal::Null)
            | (DataType::Uint16, Literal::Null)
            | (DataType::Float, Literal::Null)
            | (DataType::Decimal, Literal::Null)
            | (DataType::Text, Literal::Null) => Ok(Value::Null),
            (DataType::Date, Literal::Text(v)) => parse_date(v)
                .map(Value::Date)
                .ok_or_else(|| ValueError::LiteralCastToDateFailed(v.to_string()).into()),
            (DataType::Time, Literal::Text(v)) => parse_time(v)
                .map(Value::Time)
                .ok_or_else(|| ValueError::LiteralCastToTimeFailed(v.to_string()).into()),
            (DataType::Timestamp, Literal::Text(v)) => parse_timestamp(v)
                .map(Value::Timestamp)
                .ok_or_else(|| ValueError::LiteralCastToTimestampFailed(v.to_string()).into()),
            (DataType::Inet, Literal::Number(v)) => {
                if let Some(x) = v.to_u32() {
                    Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(x))))
                } else if let Some(x) = v.to_u128() {
                    Ok(Value::Inet(IpAddr::V6(Ipv6Addr::from(x))))
                } else {
                    Err(ValueError::FailedToParseInetString(v.to_string()).into())
                }
            }
            (DataType::Inet, Literal::Text(v)) => IpAddr::from_str(v)
                .map(Value::Inet)
                .map_err(|_| ValueError::FailedToParseInetString(v.to_string()).into()),
            _ => Err(ValueError::UnimplementedLiteralCast {
                data_type: data_type.clone(),
                literal: format!("{:?}", literal),
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{data::Literal, prelude::Value},
        bigdecimal::BigDecimal,
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        std::net::{IpAddr, Ipv4Addr, Ipv6Addr},
    };

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn date_time(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32, ms: u32) -> NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_milli_opt(hh, mm, ss, ms)
            .unwrap()
    }

    fn time(hour: u32, min: u32, sec: u32, milli: u32) -> NaiveTime {
        chrono::NaiveTime::from_hms_milli_opt(hour, min, sec, milli).unwrap()
    }

    #[test]
    fn eq() {
        use {
            super::parse_uuid,
            std::{borrow::Cow, str::FromStr},
        };

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

        let uuid_text = "936DA01F9ABD4d9d80C702AF85C822A8";
        let uuid = parse_uuid(uuid_text).unwrap();

        let bytea = || hex::decode("123456").unwrap();
        let inet = |v: &str| Value::Inet(IpAddr::from_str(v).unwrap());

        assert_eq!(Value::Bool(true), Literal::Boolean(true));
        assert_eq!(Value::I8(8), num!("8"));
        assert_eq!(Value::I16(16), num!("16"));
        assert_eq!(Value::I32(32), num!("32"));
        assert_eq!(Value::I64(64), num!("64"));
        assert_eq!(Value::I128(128), num!("128"));
        assert_eq!(Value::U8(7), num!("7"));
        assert_eq!(Value::U16(64), num!("64"));
        assert_eq!(Value::F64(7.123), num!("7.123"));
        assert_eq!(Value::Str("Hello".to_owned()), text!("Hello"));
        assert_eq!(Value::Bytea(bytea()), Literal::Bytea(bytea()));
        assert_eq!(inet("127.0.0.1"), text!("127.0.0.1"));
        assert_eq!(inet("::1"), text!("::1"));
        assert_eq!(inet("0.0.0.0"), num!("0"));
        assert_ne!(inet("::1"), num!("0"));
        assert_eq!(inet("::2:4cb0:16ea"), num!("9876543210"));
        assert_eq!(Value::Date(date(2021, 11, 20)), text!("2021-11-20"));
        assert_ne!(Value::Date(date(2021, 11, 20)), text!("202=abcdef"));
        assert_eq!(
            Value::Timestamp(date_time(2021, 11, 20, 10, 0, 0, 0)),
            text!("2021-11-20T10:00:00Z")
        );
        assert_ne!(
            Value::Timestamp(date_time(2021, 11, 20, 10, 0, 0, 0)),
            text!("2021-11-Hello")
        );
        assert_eq!(Value::Time(time(10, 0, 0, 0)), text!("10:00:00"));
        assert_ne!(Value::Time(time(10, 0, 0, 0)), text!("FALSE"));
        assert_eq!(Value::Uuid(uuid), text!(uuid_text));
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
    fn try_from_literal() {
        use {
            crate::{ast::DataType, data::ValueError},
            chrono::NaiveDate,
            rust_decimal::Decimal,
            std::{borrow::Cow, str::FromStr},
        };

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
                assert_eq!(Value::try_from_literal(&$to, &$from), Ok($expected));
            };
        }

        let bytea = |v| hex::decode(v).unwrap();
        let inet = |v| IpAddr::from_str(v).unwrap();

        test!(DataType::Boolean, Literal::Boolean(true), Value::Bool(true));
        test!(DataType::Int, num!("123456789"), Value::I64(123456789));
        test!(DataType::Int8, num!("64"), Value::I8(64));
        test!(DataType::Int16, num!("64"), Value::I16(64));
        test!(DataType::Int32, num!("64"), Value::I32(64));
        test!(DataType::Int, num!("64"), Value::I64(64));
        test!(DataType::Int128, num!("64"), Value::I128(64));
        test!(DataType::Uint8, num!("8"), Value::U8(8));
        test!(DataType::Uint16, num!("64"), Value::U16(64));

        test!(DataType::Float, num!("123456789"), Value::F64(123456789.0));
        test!(
            DataType::Text,
            text!("Good!"),
            Value::Str("Good!".to_owned())
        );
        test!(
            DataType::Bytea,
            Literal::Bytea(bytea("1234")),
            Value::Bytea(bytea("1234"))
        );
        test!(DataType::Bytea, text!("1234"), Value::Bytea(bytea("1234")));
        assert_eq!(
            Value::try_from_literal(&DataType::Bytea, &text!("123")),
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
            Value::try_from_literal(&DataType::Inet, &text!("123")),
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
            Value::Uuid(195965723427462096757863453463987888808)
        );
        test!(
            DataType::Uuid,
            Literal::Bytea(bytea("936DA01F9ABD4d9d80C702AF85C822A8")),
            Value::Uuid(195965723427462096757863453463987888808)
        );

        assert_eq!(
            Value::try_from_literal(
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
            Value::try_from_literal(
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
        use std::{borrow::Cow, str::FromStr};

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

        let bytea = |v| hex::decode(v).unwrap();

        macro_rules! test {
            ($from: expr, $expected: expr) => {
                assert_eq!(Value::try_from($from), Ok($expected));
            };
        }

        test!(text!("hello"), Value::Str("hello".to_owned()));
        test!(&text!("hallo"), Value::Str("hallo".to_owned()));
        test!(Literal::Bytea(bytea("1234")), Value::Bytea(bytea("1234")));
        test!(&Literal::Bytea(bytea("1234")), Value::Bytea(bytea("1234")));
        test!(num!("1234567890"), Value::I64(1234567890));
        test!(num!("12345678.90"), Value::F64(12345678.90));
        test!(&Literal::Boolean(false), Value::Bool(false));
        assert!(matches!(Value::try_from(&Literal::Null), Ok(Value::Null)))
    }

    #[test]
    fn try_cast_from_literal() {
        use {
            crate::{ast::DataType, data::Interval as I},
            chrono::NaiveDate,
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
            ($to: expr, $from: expr, $expected: expr) => {
                assert_eq!(Value::try_cast_from_literal(&$to, &$from), Ok($expected))
            };
        }

        macro_rules! test_null {
            ($to: expr, $from: expr) => {
                assert!(matches!(
                    Value::try_cast_from_literal(&$to, &$from),
                    Ok(Value::Null)
                ))
            };
        }

        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd_opt(y, m, d)
                .unwrap()
                .and_hms_milli_opt(hh, mm, ss, ms)
                .unwrap()
        };

        test!(
            DataType::Boolean,
            Literal::Boolean(false),
            Value::Bool(false)
        );
        test!(DataType::Boolean, text!("false"), Value::Bool(false));
        test!(DataType::Boolean, text!("true"), Value::Bool(true));
        test!(DataType::Boolean, num!("0"), Value::Bool(false));
        test!(DataType::Boolean, num!("1"), Value::Bool(true));

        test!(DataType::Int8, text!("127"), Value::I8(127));
        test!(DataType::Int8, num!("125"), Value::I8(125));
        test!(DataType::Int8, Literal::Boolean(true), Value::I8(1));
        test!(DataType::Int8, Literal::Boolean(false), Value::I8(0));

        test!(DataType::Int16, text!("127"), Value::I16(127));
        test!(DataType::Int16, num!("125"), Value::I16(125));
        test!(DataType::Int16, Literal::Boolean(true), Value::I16(1));
        test!(DataType::Int16, Literal::Boolean(false), Value::I16(0));

        test!(DataType::Int32, text!("127"), Value::I32(127));
        test!(DataType::Int32, num!("125"), Value::I32(125));
        test!(DataType::Int32, Literal::Boolean(true), Value::I32(1));
        test!(DataType::Int32, Literal::Boolean(false), Value::I32(0));

        test!(DataType::Int, text!("1234567890"), Value::I64(1234567890));
        test!(DataType::Int, num!("1234567890"), Value::I64(1234567890));
        test!(DataType::Int, Literal::Boolean(true), Value::I64(1));
        test!(DataType::Int, Literal::Boolean(false), Value::I64(0));
        test!(DataType::Int128, text!("127"), Value::I128(127));
        test!(DataType::Int128, num!("125"), Value::I128(125));
        test!(DataType::Int128, Literal::Boolean(true), Value::I128(1));
        test!(DataType::Int128, Literal::Boolean(false), Value::I128(0));

        test!(DataType::Uint8, text!("127"), Value::U8(127));
        test!(DataType::Uint8, num!("125"), Value::U8(125));
        test!(DataType::Uint8, Literal::Boolean(true), Value::U8(1));
        test!(DataType::Uint8, Literal::Boolean(false), Value::U8(0));

        test!(DataType::Uint16, text!("127"), Value::U16(127));
        test!(DataType::Uint16, num!("125"), Value::U16(125));
        test!(DataType::Uint16, Literal::Boolean(true), Value::U16(1));
        test!(DataType::Uint16, Literal::Boolean(false), Value::U16(0));

        test!(DataType::Float, text!("12345.6789"), Value::F64(12345.6789));
        test!(DataType::Float, num!("123456.789"), Value::F64(123456.789));
        test!(DataType::Float, Literal::Boolean(true), Value::F64(1.0));
        test!(DataType::Float, Literal::Boolean(false), Value::F64(0.0));
        test!(
            DataType::Text,
            num!("1234567890"),
            Value::Str("1234567890".to_owned())
        );
        test!(DataType::Text, text!("Cow"), Value::Str("Cow".to_owned()));
        test!(
            DataType::Text,
            Literal::Boolean(true),
            Value::Str("TRUE".to_owned())
        );
        test!(
            DataType::Text,
            Literal::Boolean(false),
            Value::Str("FALSE".to_owned())
        );
        test!(
            DataType::Interval,
            text!("'+22-10' YEAR TO MONTH"),
            Value::Interval(I::Month(274))
        );
        test!(
            DataType::Uuid,
            text!("936DA01F9ABD4d9d80C702AF85C822A8"),
            Value::Uuid(195965723427462096757863453463987888808)
        );
        test_null!(DataType::Boolean, Literal::Null);
        test_null!(DataType::Int, Literal::Null);
        test_null!(DataType::Int8, Literal::Null);
        test_null!(DataType::Uint8, Literal::Null);
        test_null!(DataType::Uint16, Literal::Null);
        test_null!(DataType::Float, Literal::Null);
        test_null!(DataType::Text, Literal::Null);
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
            Value::Inet(IpAddr::from(Ipv4Addr::from(1234567890)))
        );
        test!(
            DataType::Inet,
            num!("91234567890"),
            Value::Inet(IpAddr::from(Ipv6Addr::from(91234567890)))
        );
        test!(
            DataType::Inet,
            text!("::1"),
            Value::Inet(IpAddr::from_str("::1").unwrap())
        );
    }
}
