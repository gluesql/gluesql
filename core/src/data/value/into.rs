use {
    super::{
        date::{parse_date, parse_time, parse_timestamp},
        Value, ValueError,
    },
    crate::{
        data::Interval,
        result::{Error, Result},
    },
    chrono::{NaiveDate, NaiveDateTime, NaiveTime},
    rust_decimal::prelude::*,
    uuid::Uuid,
};

impl From<&Value> for String {
    fn from(v: &Value) -> Self {
        match v {
            Value::Str(value) => value.to_string(),
            Value::Bool(value) => (if *value { "TRUE" } else { "FALSE" }).to_string(),
            Value::I8(value) => value.to_string(),
            Value::I64(value) => value.to_string(),
            Value::F64(value) => value.to_string(),
            Value::Date(value) => value.to_string(),
            Value::Timestamp(value) => value.to_string(),
            Value::Time(value) => value.to_string(),
            Value::Interval(value) => String::from(value),
            Value::Uuid(value) => Uuid::from_u128(*value).to_string(),
            Value::Map(_) => "[MAP]".to_owned(),
            Value::List(_) => "[LIST]".to_owned(),
            Value::Decimal(value) => value.to_string(),
            Value::Null => String::from("NULL"),
            Value::CustomType(_) => String::from("[CUSTOM_TYPE]"),
        }
    }
}

impl From<Value> for String {
    fn from(v: Value) -> String {
        match v {
            Value::Str(value) => value,
            _ => String::from(&v),
        }
    }
}

impl TryInto<bool> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<bool> {
        Ok(match self {
            Value::Bool(value) => *value,
            Value::I8(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::I64(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::F64(value) => {
                if value.eq(&1.0) {
                    true
                } else if value.eq(&0.0) {
                    false
                } else {
                    return Err(ValueError::ImpossibleCast.into());
                }
            }
            Value::Str(value) => match value.to_uppercase().as_str() {
                "TRUE" => true,
                "FALSE" => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::Decimal(value) => {
                if value == &rust_decimal::Decimal::ONE {
                    true
                } else if value == &rust_decimal::Decimal::ZERO {
                    false
                } else {
                    return Err(ValueError::ImpossibleCast.into());
                }
            }
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Null
            | Value::CustomType(_) => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryInto<bool> for Value {
    type Error = Error;

    fn try_into(self) -> Result<bool> {
        (&self).try_into()
    }
}

impl TryInto<i8> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<i8> {
        Ok(match self {
            Value::Bool(value) => {
                if *value {
                    1
                } else {
                    0
                }
            }
            Value::I8(value) => *value,
            Value::I64(value) => *value as i8,
            Value::F64(value) => value.trunc() as i8,
            Value::Str(value) => value
                .parse::<i8>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Null
            | Value::CustomType(_) => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryInto<i8> for Value {
    type Error = Error;

    fn try_into(self) -> Result<i8> {
        (&self).try_into()
    }
}

impl TryInto<i64> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<i64> {
        Ok(match self {
            Value::Bool(value) => {
                if *value {
                    1
                } else {
                    0
                }
            }
            Value::I8(value) => *value as i64,
            Value::I64(value) => *value,
            Value::F64(value) => value.trunc() as i64,
            Value::Str(value) => value
                .parse::<i64>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_i64().ok_or(ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Null
            | Value::CustomType(_) => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryInto<i64> for Value {
    type Error = Error;

    fn try_into(self) -> Result<i64> {
        (&self).try_into()
    }
}

impl TryInto<f64> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<f64> {
        Ok(match self {
            Value::Bool(value) => {
                if *value {
                    1.0
                } else {
                    0.0
                }
            }
            Value::I8(value) => *value as f64,
            Value::I64(value) => (*value as f64).trunc(),
            Value::F64(value) => *value,
            Value::Str(value) => value
                .parse::<f64>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_f64().ok_or(ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Null
            | Value::CustomType(_) => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryInto<f64> for Value {
    type Error = Error;

    fn try_into(self) -> Result<f64> {
        (&self).try_into()
    }
}

impl TryInto<Decimal> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<Decimal> {
        Ok(match self {
            Value::Bool(value) => {
                if *value {
                    Decimal::ONE
                } else {
                    Decimal::ZERO
                }
            }
            Value::I8(value) => Decimal::from_i8(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => Decimal::from_i64(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::F64(value) => Decimal::from_f64(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::Str(value) => {
                Decimal::from_str(value).map_err(|_| ValueError::ImpossibleCast)?
            }
            Value::Decimal(value) => *value,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Null
            | Value::CustomType(_) => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryInto<Decimal> for Value {
    type Error = Error;

    fn try_into(self) -> Result<Decimal> {
        (&self).try_into()
    }
}

impl TryInto<NaiveDate> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<NaiveDate> {
        Ok(match self {
            Value::Date(value) => *value,
            Value::Timestamp(value) => value.date(),
            Value::Str(value) => parse_date(value).ok_or(ValueError::ImpossibleCast)?,
            _ => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryInto<NaiveTime> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<NaiveTime> {
        Ok(match self {
            Value::Time(value) => *value,
            Value::Str(value) => parse_time(value).ok_or(ValueError::ImpossibleCast)?,
            _ => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryInto<NaiveDateTime> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<NaiveDateTime> {
        Ok(match self {
            Value::Date(value) => value.and_hms(0, 0, 0),
            Value::Str(value) => parse_timestamp(value).ok_or(ValueError::ImpossibleCast)?,
            Value::Timestamp(value) => *value,
            _ => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryInto<Interval> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<Interval> {
        match self {
            Value::Str(value) => Interval::try_from(value.as_str()),
            _ => Err(ValueError::ImpossibleCast.into()),
        }
    }
}

impl TryInto<u128> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<u128> {
        match self {
            Value::Uuid(value) => Ok(*value),
            _ => Err(ValueError::ImpossibleCast.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Value, ValueError},
        crate::{data::Interval as I, result::Result},
        chrono,
        rust_decimal::Decimal,
        std::collections::HashMap,
    };

    #[test]
    fn from() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!(String::from($from), $to.to_owned())
            };
        }
        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(hh, mm, ss, ms)
        };
        let time = chrono::NaiveTime::from_hms_milli;
        let date = chrono::NaiveDate::from_ymd;
        test!(Value::Str("text".to_owned()), "text");
        test!(Value::Bool(true), "TRUE");
        test!(Value::I8(122), "122");
        test!(Value::I64(1234567890), "1234567890");
        test!(Value::F64(1234567890.0987), "1234567890.0987");
        test!(Value::Date(date(2021, 11, 20)), "2021-11-20");
        test!(
            Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
            "2021-11-20 10:00:00"
        );
        test!(Value::Time(time(10, 0, 0, 0)), "10:00:00");
        test!(Value::Interval(I::Month(1)), String::from(I::Month(1)));
        test!(
            Value::Uuid(195965723427462096757863453463987888808),
            "936da01f-9abd-4d9d-80c7-02af85c822a8"
        );
        test!(Value::Map(HashMap::new()), "[MAP]");
        test!(Value::List(Vec::new()), "[LIST]");
        test!(Value::Decimal(Decimal::new(2000, 1)), "200.0");
        test!(Value::Null, "NULL");
    }

    #[test]
    fn try_into_bool() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<bool>, $to)
            };
        }
        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(hh, mm, ss, ms)
        };
        let time = chrono::NaiveTime::from_hms_milli;
        let date = chrono::NaiveDate::from_ymd;
        test!(Value::Bool(true), Ok(true));
        test!(Value::I8(1), Ok(true));
        test!(Value::I8(0), Ok(false));
        test!(Value::I64(1), Ok(true));
        test!(Value::I64(0), Ok(false));
        test!(Value::F64(1.0), Ok(true));
        test!(Value::F64(0.0), Ok(false));
        test!(Value::Str("true".to_owned()), Ok(true));
        test!(Value::Str("false".to_owned()), Ok(false));
        test!(Value::Decimal(Decimal::new(10, 1)), Ok(true));
        test!(Value::Decimal(Decimal::new(0, 1)), Ok(false));
        test!(
            Value::Decimal(Decimal::new(2, 0)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Date(date(2021, 11, 20)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Time(time(10, 0, 0, 0)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Interval(I::Month(1)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Uuid(195965723427462096757863453463987888808),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Map(HashMap::new()),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::List(Vec::new()),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
    }

    #[test]
    fn try_into_i8() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i8>, $to)
            };
        }
        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(hh, mm, ss, ms)
        };
        let time = chrono::NaiveTime::from_hms_milli;
        let date = chrono::NaiveDate::from_ymd;
        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::F64(122.0), Ok(122));
        test!(Value::F64(122.1), Ok(122));
        test!(Value::Str("122".to_owned()), Ok(122));
        test!(Value::Decimal(Decimal::new(123, 0)), Ok(123));
        test!(
            Value::Date(date(2021, 11, 20)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Time(time(10, 0, 0, 0)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Interval(I::Month(1)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Uuid(195965723427462096757863453463987888808),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Map(HashMap::new()),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::List(Vec::new()),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
    }

    #[test]
    fn try_into_i64() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i64>, $to)
            };
        }
        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(hh, mm, ss, ms)
        };
        let time = chrono::NaiveTime::from_hms_milli;
        let date = chrono::NaiveDate::from_ymd;
        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I64(1234567890), Ok(1234567890));
        test!(Value::F64(1234567890.0), Ok(1234567890));
        test!(Value::F64(1234567890.1), Ok(1234567890));
        test!(Value::Str("1234567890".to_owned()), Ok(1234567890));
        test!(Value::Decimal(Decimal::new(1234567890, 0)), Ok(1234567890));
        test!(
            Value::Date(date(2021, 11, 20)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Time(time(10, 0, 0, 0)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Interval(I::Month(1)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Uuid(195965723427462096757863453463987888808),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Map(HashMap::new()),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::List(Vec::new()),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
    }

    #[test]
    fn try_into_f64() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<f64>, $to)
            };
        }
        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(hh, mm, ss, ms)
        };
        let time = chrono::NaiveTime::from_hms_milli;
        let date = chrono::NaiveDate::from_ymd;
        test!(Value::Bool(true), Ok(1.0));
        test!(Value::Bool(false), Ok(0.0));
        test!(Value::I8(122), Ok(122.0));
        test!(Value::I64(122), Ok(122.0));
        test!(Value::I64(1234567890), Ok(1234567890.0));
        test!(Value::F64(1234567890.1), Ok(1234567890.1));
        test!(Value::Str("1234567890.1".to_owned()), Ok(1234567890.1));
        test!(
            Value::Decimal(Decimal::new(12345678901, 1)),
            Ok(1234567890.1)
        );
        test!(
            Value::Date(date(2021, 11, 20)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Time(time(10, 0, 0, 0)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Interval(I::Month(1)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Uuid(195965723427462096757863453463987888808),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Map(HashMap::new()),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::List(Vec::new()),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
    }

    #[test]
    fn try_into_naive_date() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<chrono::NaiveDate>, $to)
            };
        }
        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(hh, mm, ss, ms)
        };
        let date = chrono::NaiveDate::from_ymd;
        test!(&Value::Date(date(2021, 11, 20)), Ok(date(2021, 11, 20)));
        test!(
            &Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
            Ok(date(2021, 11, 20))
        );
        test!(&Value::Str("2021-11-20".to_owned()), Ok(date(2021, 11, 20)));
    }

    #[test]
    fn try_into_naive_time() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<chrono::NaiveTime>, $to)
            };
        }
        let time = chrono::NaiveTime::from_hms_milli;
        test!(&Value::Time(time(10, 0, 0, 0)), Ok(time(10, 0, 0, 0)));
        test!(&Value::Str("10:00:00".to_owned()), Ok(time(10, 0, 0, 0)));
    }

    #[test]
    fn try_into_naive_date_time() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<chrono::NaiveDateTime>, $to)
            };
        }
        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(hh, mm, ss, ms)
        };
        let date = chrono::NaiveDate::from_ymd;
        let datetime = chrono::NaiveDateTime::new;
        let time = chrono::NaiveTime::from_hms_milli;
        test!(
            &Value::Date(date(2021, 11, 20)),
            Ok(datetime(date(2021, 11, 20), time(0, 0, 0, 0)))
        );
        test!(
            &Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
            Ok(datetime(date(2021, 11, 20), time(10, 0, 0, 0)))
        );
        test!(
            &Value::Str("2021-11-20".to_owned()),
            Ok(datetime(date(2021, 11, 20), time(0, 0, 0, 0)))
        );
    }

    #[test]
    fn try_into_interval() {
        assert_eq!(
            (&Value::Str("\"+22-10\" YEAR TO MONTH".to_owned())).try_into() as Result<I>,
            Ok(I::Month(274))
        )
    }

    #[test]
    fn try_into_u128() {
        let uuid = 195965723427462096757863453463987888808;
        assert_eq!((&Value::Uuid(uuid)).try_into() as Result<u128>, Ok(uuid))
    }
}
