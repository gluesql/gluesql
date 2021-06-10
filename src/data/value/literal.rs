use {
    super::{error::ValueError, Value},
    crate::{
        ast::DataType,
        data::Literal,
        result::{Error, Result},
    },
    chrono::{offset::Utc, DateTime, NaiveDate, NaiveDateTime, NaiveTime},
    std::{cmp::Ordering, convert::TryFrom},
};

impl PartialEq<Literal<'_>> for Value {
    fn eq(&self, other: &Literal<'_>) -> bool {
        match (self, other) {
            (Value::Bool(l), Literal::Boolean(r)) => l == r,
            (Value::I64(l), Literal::Number(r)) => match r.parse::<i64>() {
                Ok(r) => l == &r,
                Err(_) => match r.parse::<f64>() {
                    Ok(r) => (*l as f64) == r,
                    Err(_) => false,
                },
            },
            (Value::F64(l), Literal::Number(r)) => match r.parse::<f64>() {
                Ok(r) => l == &r,
                Err(_) => match r.parse::<i64>() {
                    Ok(r) => *l == (r as f64),
                    Err(_) => false,
                },
            },
            (Value::Str(l), Literal::Text(r)) => l == r.as_ref(),
            (Value::Date(l), Literal::Text(r)) => match r.parse::<NaiveDate>() {
                Ok(r) => l == &r,
                Err(_) => false,
            },
            (Value::Timestamp(l), Literal::Text(r)) => match parse_timestamp(r) {
                Ok(r) => l == &r,
                Err(_) => false,
            },
            (Value::Time(l), Literal::Text(r)) => match parse_time(r) {
                Ok(r) => l == &r,
                Err(_) => false,
            },
            (Value::Interval(l), Literal::Interval(r)) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<Literal<'_>> for Value {
    fn partial_cmp(&self, other: &Literal<'_>) -> Option<Ordering> {
        match (self, other) {
            (Value::I64(l), Literal::Number(r)) => match r.parse::<i64>() {
                Ok(r) => Some(l.cmp(&r)),
                Err(_) => match r.parse::<f64>() {
                    Ok(r) => (*l as f64).partial_cmp(&r),
                    Err(_) => None,
                },
            },
            (Value::F64(l), Literal::Number(r)) => match r.parse::<f64>() {
                Ok(r) => l.partial_cmp(&r),
                Err(_) => match r.parse::<i64>() {
                    Ok(r) => l.partial_cmp(&(r as f64)),
                    Err(_) => None,
                },
            },
            (Value::Str(l), Literal::Text(r)) => Some(l.cmp(r.as_ref())),
            (Value::Date(l), Literal::Text(r)) => match r.parse::<NaiveDate>() {
                Ok(r) => l.partial_cmp(&r),
                Err(_) => None,
            },
            (Value::Timestamp(l), Literal::Text(r)) => match parse_timestamp(r) {
                Ok(r) => l.partial_cmp(&r),
                Err(_) => None,
            },
            (Value::Time(l), Literal::Text(r)) => match parse_time(r) {
                Ok(r) => l.partial_cmp(&r),
                Err(_) => None,
            },
            (Value::Interval(l), Literal::Interval(r)) => l.partial_cmp(&r),
            _ => None,
        }
    }
}

impl TryFrom<&Literal<'_>> for Value {
    type Error = Error;

    fn try_from(literal: &Literal<'_>) -> Result<Self> {
        match literal {
            Literal::Number(v) => v
                .parse::<i64>()
                .map_or_else(|_| v.parse::<f64>().map(Value::F64), |v| Ok(Value::I64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
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
            Literal::Number(v) => v
                .parse::<i64>()
                .map_or_else(|_| v.parse::<f64>().map(Value::F64), |v| Ok(Value::I64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            Literal::Boolean(v) => Ok(Value::Bool(v)),
            Literal::Text(v) => Ok(Value::Str(v.into_owned())),
            Literal::Interval(v) => Ok(Value::Interval(v)),
            Literal::Null => Ok(Value::Null),
        }
    }
}

impl Value {
    pub fn try_from_literal(data_type: &DataType, literal: &Literal<'_>) -> Result<Value> {
        match (data_type, literal) {
            (DataType::Boolean, Literal::Boolean(v)) => Ok(Value::Bool(*v)),
            (DataType::Int, Literal::Number(v)) => v
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (DataType::Float, Literal::Number(v)) => v
                .parse::<f64>()
                .map(Value::F64)
                .map_err(|_| ValueError::UnreachableNumberParsing.into()),
            (DataType::Text, Literal::Text(v)) => Ok(Value::Str(v.to_string())),
            (DataType::Date, Literal::Text(v)) => v
                .parse::<NaiveDate>()
                .map(Value::Date)
                .map_err(|_| ValueError::FailedToParseDate(v.to_string()).into()),
            (DataType::Timestamp, Literal::Text(v)) => parse_timestamp(v)
                .map(Value::Timestamp)
                .map_err(|_| ValueError::FailedToParseTimestamp(v.to_string()).into()),
            (DataType::Time, Literal::Text(v)) => parse_time(v)
                .map(Value::Time)
                .map_err(|_| ValueError::FailedToParseTime(v.to_string()).into()),
            (DataType::Interval, Literal::Interval(v)) => Ok(Value::Interval(*v)),
            (DataType::Boolean, Literal::Null)
            | (DataType::Int, Literal::Null)
            | (DataType::Float, Literal::Null)
            | (DataType::Text, Literal::Null)
            | (DataType::Date, Literal::Null)
            | (DataType::Timestamp, Literal::Null)
            | (DataType::Time, Literal::Null)
            | (DataType::Interval, Literal::Null) => Ok(Value::Null),
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
            (DataType::Boolean, Literal::Text(v)) | (DataType::Boolean, Literal::Number(v)) => {
                match v.to_uppercase().as_str() {
                    "TRUE" | "1" => Ok(Value::Bool(true)),
                    "FALSE" | "0" => Ok(Value::Bool(false)),
                    _ => Err(ValueError::LiteralCastToBooleanFailed(v.to_string()).into()),
                }
            }
            (DataType::Int, Literal::Text(v)) => v
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| ValueError::LiteralCastFromTextToIntegerFailed(v.to_string()).into()),
            (DataType::Int, Literal::Number(v)) => v
                .parse::<f64>()
                .map_err(|_| {
                    ValueError::UnreachableLiteralCastFromNumberToInteger(v.to_string()).into()
                })
                .map(|v| Value::I64(v.trunc() as i64)),
            (DataType::Int, Literal::Boolean(v)) => {
                let v = if *v { 1 } else { 0 };

                Ok(Value::I64(v))
            }
            (DataType::Float, Literal::Text(v)) | (DataType::Float, Literal::Number(v)) => v
                .parse::<f64>()
                .map(Value::F64)
                .map_err(|_| ValueError::LiteralCastToFloatFailed(v.to_string()).into()),
            (DataType::Float, Literal::Boolean(v)) => {
                let v = if *v { 1.0 } else { 0.0 };

                Ok(Value::F64(v))
            }
            (DataType::Text, Literal::Number(v)) | (DataType::Text, Literal::Text(v)) => {
                Ok(Value::Str(v.to_string()))
            }
            (DataType::Text, Literal::Boolean(v)) => {
                let v = if *v { "TRUE" } else { "FALSE" };

                Ok(Value::Str(v.to_owned()))
            }
            (DataType::Boolean, Literal::Null)
            | (DataType::Int, Literal::Null)
            | (DataType::Float, Literal::Null)
            | (DataType::Text, Literal::Null) => Ok(Value::Null),
            _ => Err(ValueError::UnimplementedLiteralCast {
                data_type: data_type.clone(),
                literal: format!("{:?}", literal),
            }
            .into()),
        }
    }
}

fn parse_timestamp(v: &str) -> Result<NaiveDateTime> {
    if let Ok(v) = v.parse::<DateTime<Utc>>() {
        return Ok(v.naive_utc());
    } else if let Ok(v) = v.parse::<NaiveDateTime>() {
        return Ok(v);
    } else if let Ok(v) = v.parse::<NaiveDate>() {
        return Ok(v.and_hms(0, 0, 0));
    }

    let forms = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%.f"];

    for form in forms.iter() {
        if let Ok(v) = NaiveDateTime::parse_from_str(&v, form) {
            return Ok(v);
        }
    }

    Err(ValueError::FailedToParseTimestamp(v.to_string()).into())
}

fn parse_time(v: &str) -> Result<NaiveTime> {
    if let Ok(v) = v.parse::<NaiveTime>() {
        return Ok(v);
    }

    let forms = [
        "%P %I:%M",
        "%P %l:%M",
        "%P %I:%M:%S",
        "%P %l:%M:%S",
        "%P %I:%M:%S%.f",
        "%P %l:%M:%S%.f",
        "%I:%M %P",
        "%l:%M %P",
        "%I:%M:%S %P",
        "%l:%M:%S %P",
        "%I:%M:%S%.f %P",
        "%l:%M:%S%.f %P",
    ];

    let v = v.to_uppercase();

    for form in forms.iter() {
        if let Ok(v) = NaiveTime::parse_from_str(&v, form) {
            return Ok(v);
        }
    }

    Err(ValueError::FailedToParseTime(v).into())
}

#[cfg(test)]
mod tests {
    #[test]
    fn timestamp() {
        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(hh, mm, ss, ms)
        };

        macro_rules! test (
            ($timestamp: literal, $result: expr) => {
                assert_eq!(super::parse_timestamp($timestamp), Ok($result));
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
        let time = |h, m, s, ms| chrono::NaiveTime::from_hms_milli(h, m, s, ms);

        macro_rules! test (
            ($time: literal, $result: expr) => {
                assert_eq!(super::parse_time($time), Ok($result));
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
