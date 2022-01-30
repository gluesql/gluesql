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
    std::cmp::Ordering,
};

impl PartialEq<Literal<'_>> for Value {
    fn eq(&self, other: &Literal<'_>) -> bool {
        match (self, other) {
            (Value::Bool(l), Literal::Boolean(r)) => l == r,
            (Value::I8(l), Literal::Number(r)) => r.to_i8().map(|r| *l == r).unwrap_or(false),
            (Value::I64(l), Literal::Number(r)) => r.to_i64().map(|r| *l == r).unwrap_or(false),
            (Value::F64(l), Literal::Number(r)) => r.to_f64().map(|r| *l == r).unwrap_or(false),
            (Value::Str(l), Literal::Text(r)) => l == r.as_ref(),
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
            (Value::Interval(l), Literal::Interval(r)) => l == r,
            (Value::Uuid(l), Literal::Text(r)) => parse_uuid(r).map(|r| l == &r).unwrap_or(false),
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
            (Value::I64(l), Literal::Number(r)) => {
                r.to_i64().map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            (Value::F64(l), Literal::Number(r)) => {
                r.to_f64().map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            (Value::Str(l), Literal::Text(r)) => Some(l.cmp(r.as_ref())),
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
            (Value::Interval(l), Literal::Interval(r)) => l.partial_cmp(r),
            (Value::Uuid(l), Literal::Text(r)) => {
                parse_uuid(r).map(|r| l.partial_cmp(&r)).unwrap_or(None)
            }
            _ => None,
        }
    }
}

impl TryFrom<&Literal<'_>> for Value {
    type Error = Error;

    fn try_from(literal: &Literal<'_>) -> Result<Self> {
        match literal {
            Literal::Number(v) => v
                .to_i64()
                .map(Value::I64)
                .or_else(|| v.to_f64().map(Value::F64))
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            Literal::Boolean(v) => Ok(Value::Bool(*v)),
            Literal::Text(v) => Ok(Value::Str(v.as_ref().to_owned())),
            Literal::Interval(v) => Ok(Value::Interval(*v)),
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
        match (data_type, literal) {
            (DataType::Boolean, Literal::Boolean(v)) => Ok(Value::Bool(*v)),
            (DataType::Int, Literal::Number(v)) => v
                .to_i64()
                .map(Value::I64)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Int8, Literal::Number(v)) => v
                .to_i8()
                .map(Value::I8)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Float, Literal::Number(v)) => v
                .to_f64()
                .map(Value::F64)
                .ok_or_else(|| ValueError::UnreachableNumberParsing.into()),
            (DataType::Text, Literal::Text(v)) => Ok(Value::Str(v.to_string())),
            (DataType::Date, Literal::Text(v)) => v
                .parse::<NaiveDate>()
                .map(Value::Date)
                .map_err(|_| ValueError::FailedToParseDate(v.to_string()).into()),
            (DataType::Timestamp, Literal::Text(v)) => parse_timestamp(v)
                .map(Value::Timestamp)
                .ok_or_else(|| ValueError::FailedToParseTimestamp(v.to_string()).into()),
            (DataType::Time, Literal::Text(v)) => parse_time(v)
                .map(Value::Time)
                .ok_or_else(|| ValueError::FailedToParseTime(v.to_string()).into()),
            (DataType::Interval, Literal::Interval(v)) => Ok(Value::Interval(*v)),
            (DataType::Uuid, Literal::Text(v)) => parse_uuid(v).map(Value::Uuid),
            (DataType::Map, Literal::Text(v)) => Value::parse_json_map(v),
            (DataType::List, Literal::Text(v)) => Value::parse_json_list(v),
            (DataType::Decimal(p, s), Literal::Number(v)) => Value::parse_decimal(p,s,v),
            // v.to_string()
            //     .parse::<Decimal>()
            //     .map(Value::Decimal)
            //     .map_err(|_| ValueError::FailedToParseDecimal(v.to_string()).into()),
            (_, Literal::Null) => Ok(Value::Null),
            _ => Err(ValueError::IncompatibleLiteralForDataType {
                data_type: data_type.clone(),
                literal: format!("{:?}", literal),
            }
            .into()),
        }
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
            (DataType::Int, Literal::Text(v)) => v
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| ValueError::LiteralCastFromTextToIntegerFailed(v.to_string()).into()),
            (DataType::Int, Literal::Number(v)) => v
                .to_f64()
                .map(|v| Value::I64(v.trunc() as i64))
                .ok_or_else(|| {
                    ValueError::UnreachableLiteralCastFromNumberToInteger(v.to_string()).into()
                }),
            (DataType::Int, Literal::Boolean(v)) => {
                let v = if *v { 1 } else { 0 };

                Ok(Value::I64(v))
            }
            (DataType::Int8, Literal::Text(v)) => v
                .parse::<i8>()
                .map(Value::I8)
                .map_err(|_| ValueError::LiteralCastFromTextToIntegerFailed(v.to_string()).into()),
            (DataType::Int8, Literal::Number(v)) => v
                .to_f64()
                .map(|v| Value::I8(v.trunc() as i8))
                .ok_or_else(|| {
                    ValueError::UnreachableLiteralCastFromNumberToInteger(v.to_string()).into()
                }),
            (DataType::Int8, Literal::Boolean(v)) => {
                let v = if *v { 1 } else { 0 };

                Ok(Value::I8(v))
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
            (DataType::Text, Literal::Number(v)) => Ok(Value::Str(v.to_string())),
            (DataType::Text, Literal::Text(v)) => Ok(Value::Str(v.to_string())),
            (DataType::Text, Literal::Boolean(v)) => {
                let v = if *v { "TRUE" } else { "FALSE" };

                Ok(Value::Str(v.to_owned()))
            }
            (DataType::Interval, Literal::Text(v)) => {
                Interval::try_from(v.as_str()).map(Value::Interval)
            }
            (DataType::Uuid, Literal::Text(v)) => parse_uuid(v).map(Value::Uuid),
            (DataType::Boolean, Literal::Null)
            | (DataType::Int, Literal::Null)
            | (DataType::Int8, Literal::Null)
            | (DataType::Float, Literal::Null)
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
    use crate::{data::Literal, prelude::Value};
    use bigdecimal::BigDecimal;

    #[test]
    fn eq() {
        use super::parse_uuid;
        use crate::data::interval::Interval as I;
        use std::borrow::Cow;
        use std::str::FromStr;

        let date = chrono::NaiveDate::from_ymd;

        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(hh, mm, ss, ms)
        };

        let time = chrono::NaiveTime::from_hms_milli;

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

        assert_eq!(Value::Bool(true), Literal::Boolean(true));
        assert_eq!(Value::I64(1), num!("1"));
        assert_eq!(Value::F64(7.123), num!("7.123"));
        assert_eq!(Value::Str("Hello".to_owned()), text!("Hello"));
        assert_eq!(Value::Date(date(2021, 11, 20)), text!("2021-11-20"));
        assert_ne!(Value::Date(date(2021, 11, 20)), text!("202=abcdef"));
        assert_eq!(
            Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
            text!("2021-11-20T10:00:00Z")
        );
        assert_ne!(
            Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
            text!("2021-11-Hello")
        );
        assert_eq!(Value::Time(time(10, 0, 0, 0)), text!("10:00:00"));
        assert_ne!(Value::Time(time(10, 0, 0, 0)), text!("FALSE"));
        assert_eq!(Value::Interval(I::Month(1)), Literal::Interval(I::Month(1)));
        assert_eq!(Value::Uuid(uuid), text!(uuid_text));
    }

    #[test]
    fn timestamp() {
        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(hh, mm, ss, ms)
        };

        macro_rules! test (
            ($timestamp: literal, $result: expr) => {
                assert_eq!(super::parse_timestamp($timestamp), Some($result));
            }
        );

        test!("2022-12-20T10:00:00Z", timestamp(2022, 12, 20, 10, 0, 0, 0));
        test!(
            "2022-12-20T10:00:00.132Z",
            timestamp(2022, 12, 20, 10, 0, 0, 132)
        );
        test!(
            "2022-12-20T10:00:00.132+09:00",
            timestamp(2022, 12, 20, 1, 0, 0, 132)
        );
        test!("2022-11-21", timestamp(2022, 11, 21, 0, 0, 0, 0));
        test!("2022-12-20T10:00:00", timestamp(2022, 12, 20, 10, 0, 0, 0));
        test!("2022-12-20 10:00:00Z", timestamp(2022, 12, 20, 10, 0, 0, 0));
        test!("2022-12-20 10:00:00", timestamp(2022, 12, 20, 10, 0, 0, 0));
        test!(
            "2022-12-20 10:00:00.987",
            timestamp(2022, 12, 20, 10, 0, 0, 987)
        );
    }

    #[test]
    fn time() {
        let time = chrono::NaiveTime::from_hms_milli;

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
}
