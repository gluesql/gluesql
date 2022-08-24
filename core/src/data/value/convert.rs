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
    rust_decimal::prelude::{Decimal, FromPrimitive, FromStr, ToPrimitive},
    uuid::Uuid,
};

impl From<&Value> for String {
    fn from(v: &Value) -> Self {
        match v {
            Value::Str(value) => value.to_string(),
            Value::Bytea(value) => hex::encode(value),
            Value::Bool(value) => (if *value { "TRUE" } else { "FALSE" }).to_string(),
            Value::I8(value) => value.to_string(),
            Value::I16(value) => value.to_string(),
            Value::I32(value) => value.to_string(),
            Value::I64(value) => value.to_string(),
            Value::I128(value) => value.to_string(),
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

impl TryFrom<&Value> for bool {
    type Error = Error;

    fn try_from(v: &Value) -> Result<Self> {
        Ok(match v {
            Value::Bool(value) => *value,
            Value::I8(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::I16(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::I32(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::I64(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::I128(value) => match value {
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
            | Value::Bytea(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for i8 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<i8> {
        Ok(match v {
            Value::Bool(value) => {
                if *value {
                    1
                } else {
                    0
                }
            }
            Value::I8(value) => *value,
            Value::I16(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::I32(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::F64(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
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
            | Value::Bytea(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for i16 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<i16> {
        Ok(match v {
            Value::Bool(value) => {
                if *value {
                    1
                } else {
                    0
                }
            }
            Value::I8(value) => *value as i16,
            Value::I16(value) => *value,
            Value::I32(value) => *value as i16,
            Value::I64(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
            Value::F64(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
            Value::Str(value) => value
                .parse::<i16>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Bytea(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for i32 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<i32> {
        Ok(match v {
            Value::Bool(value) => {
                if *value {
                    1
                } else {
                    0
                }
            }
            Value::I8(value) => *value as i32,
            Value::I16(value) => *value as i32,
            Value::I32(value) => *value,
            Value::I64(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
            Value::F64(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
            Value::Str(value) => value
                .parse::<i32>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Bytea(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for i64 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<i64> {
        Ok(match v {
            Value::Bool(value) => {
                if *value {
                    1
                } else {
                    0
                }
            }
            Value::I8(value) => *value as i64,
            Value::I16(value) => *value as i64,
            Value::I32(value) => *value as i64,
            Value::I64(value) => *value,
            Value::I128(value) => value.to_i64().ok_or(ValueError::ImpossibleCast)?,
            Value::F64(value) => value.to_i64().ok_or(ValueError::ImpossibleCast)?,
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
            | Value::Bytea(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for i128 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<i128> {
        Ok(match v {
            Value::Bool(value) => {
                if *value {
                    1
                } else {
                    0
                }
            }
            Value::I8(value) => *value as i128,
            Value::I16(value) => *value as i128,
            Value::I32(value) => *value as i128,
            Value::I64(value) => *value as i128,
            Value::I128(value) => *value,
            Value::F64(value) => value.to_i128().ok_or(ValueError::ImpossibleCast)?,
            Value::Str(value) => value
                .parse::<i128>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_i128().ok_or(ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Bytea(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for f64 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<f64> {
        Ok(match v {
            Value::Bool(value) => {
                if *value {
                    1.0
                } else {
                    0.0
                }
            }
            Value::I8(value) => *value as f64,
            Value::I16(value) => *value as f64,
            Value::I32(value) => value.to_f64().ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => value.to_f64().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => *value as f64,
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
            | Value::Bytea(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for usize {
    type Error = Error;

    fn try_from(v: &Value) -> Result<usize> {
        Ok(match v {
            Value::Bool(value) => {
                if *value {
                    1
                } else {
                    0
                }
            }
            Value::I8(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::I16(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::I32(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::F64(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::Str(value) => value
                .parse::<usize>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Bytea(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for Decimal {
    type Error = Error;

    fn try_from(v: &Value) -> Result<Decimal> {
        Ok(match v {
            Value::Bool(value) => {
                if *value {
                    Decimal::ONE
                } else {
                    Decimal::ZERO
                }
            }
            Value::I8(value) => Decimal::from_i8(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::I16(value) => Decimal::from_i16(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::I32(value) => Decimal::from_i32(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => Decimal::from_i64(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => Decimal::from_i128(*value).ok_or(ValueError::ImpossibleCast)?,
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
            | Value::Bytea(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

// implies `TryFrom<Value> for T` from `TryFrom<&Value> for T`
macro_rules! try_from_owned_value {
    ($($target:ty), *) => {$(
        impl TryFrom<Value> for $target {
            type Error = Error;

            fn try_from(v: Value) -> Result<Self> {
                Self::try_from(&v)
            }
        }
    )*}
}

try_from_owned_value!(bool, i8, i16, i32, i64, i128, f64, u128, Decimal);

impl TryFrom<&Value> for NaiveDate {
    type Error = Error;

    fn try_from(v: &Value) -> Result<NaiveDate> {
        Ok(match v {
            Value::Date(value) => *value,
            Value::Timestamp(value) => value.date(),
            Value::Str(value) => parse_date(value).ok_or(ValueError::ImpossibleCast)?,
            _ => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for NaiveTime {
    type Error = Error;

    fn try_from(v: &Value) -> Result<NaiveTime> {
        Ok(match v {
            Value::Time(value) => *value,
            Value::Str(value) => parse_time(value).ok_or(ValueError::ImpossibleCast)?,
            _ => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for NaiveDateTime {
    type Error = Error;

    fn try_from(v: &Value) -> Result<NaiveDateTime> {
        Ok(match v {
            Value::Date(value) => value.and_hms(0, 0, 0),
            Value::Str(value) => parse_timestamp(value).ok_or(ValueError::ImpossibleCast)?,
            Value::Timestamp(value) => *value,
            _ => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for Interval {
    type Error = Error;

    fn try_from(v: &Value) -> Result<Interval> {
        match v {
            Value::Str(value) => Interval::try_from(value.as_str()),
            _ => Err(ValueError::ImpossibleCast.into()),
        }
    }
}

impl TryFrom<&Value> for u128 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<u128> {
        match v {
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
        test!(Value::Bytea(hex::decode("1234").unwrap()), "1234");
        test!(Value::Bool(true), "TRUE");
        test!(Value::I8(122), "122");
        test!(Value::I16(122), "122");
        test!(Value::I32(122), "122");
        test!(Value::I64(1234567890), "1234567890");
        test!(Value::I128(1234567890), "1234567890");
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
                assert_eq!($from.try_into() as Result<bool>, $to);
                assert_eq!(bool::try_from($from), $to);
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
        test!(Value::I16(1), Ok(true));
        test!(Value::I16(0), Ok(false));
        test!(Value::I32(1), Ok(true));
        test!(Value::I32(0), Ok(false));
        test!(Value::I64(1), Ok(true));
        test!(Value::I64(0), Ok(false));
        test!(Value::I128(1), Ok(true));
        test!(Value::I128(0), Ok(false));

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

        // impossible casts
        test!(Value::I8(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I16(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I32(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I64(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I128(3), Err(ValueError::ImpossibleCast.into()));
    }

    #[test]
    fn try_into_i8() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i8>, $to);
                assert_eq!(i8::try_from($from), $to);
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
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::F64(122.0), Ok(122));
        test!(Value::F64(122.9), Ok(122));
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

        // impossible casts to i8
        test!(Value::I16(128), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I32(128), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I64(128), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I128(128), Err(ValueError::ImpossibleCast.into()));
        test!(Value::F64(128.0), Err(ValueError::ImpossibleCast.into()));
    }

    #[test]
    fn try_into_i16() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i16>, $to);
                assert_eq!(i16::try_from($from), $to);
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
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::F64(122.0), Ok(122));
        test!(Value::F64(122.1), Ok(122));
        test!(Value::Str("122".to_owned()), Ok(122));
        test!(Value::Decimal(Decimal::new(122, 0)), Ok(122));
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
    fn try_into_i32() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i32>, $to);
                assert_eq!(i32::try_from($from), $to);
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
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
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
    fn try_into_i64() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i64>, $to);
                assert_eq!(i64::try_from($from), $to);
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
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
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
    fn try_into_i128() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i128>, $to);
                assert_eq!(i128::try_from($from), $to);
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
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::I64(1234567890), Ok(1234567890));
        test!(Value::F64(1234567890.0), Ok(1234567890));
        test!(Value::F64(1234567890.9), Ok(1234567890));
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
                assert_eq!($from.try_into() as Result<f64>, $to);
                assert_eq!(f64::try_from($from), $to);
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
        test!(Value::I16(122), Ok(122.0));
        test!(Value::I32(122), Ok(122.0));
        test!(Value::I64(122), Ok(122.0));
        test!(Value::I128(122), Ok(122.0));
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
    fn try_into_usize() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<usize>, $to);
                assert_eq!(usize::try_from($from), $to);
                // rustc: the trait bound `usize: From<data::value::Value>` is not satisfied
                // the following other types implement trait `From<T>`:
                //   <f32 as From<i16>>
                //   <f32 as From<i8>>
                //   <f32 as From<u16>>
                //   <f32 as From<u8>>
                //   <f64 as From<f32>>
                //   <f64 as From<i16>>
                //   <f64 as From<i32>>
                //   <f64 as From<i8>>
                // and 67 others
                // required because of the requirements on the impl of `Into<usize>` for `data::value::Value`
                // required because of the requirements on the impl of `TryFrom<data::value::Value>` for `usize` [E0277]
                // rustc: the trait bound `usize: From<data::value::Value>` is not satisfied
                // the following other types implement trait `From<T>`:
                //   <f32 as From<i16>>
                //   <f32 as From<i8>>
                //   <f32 as From<u16>>
                //   <f32 as From<u8>>
                //   <f64 as From<f32>>
                //   <f64 as From<i16>>
                //   <f64 as From<i32>>
                //   <f64 as From<i8>>
                // and 67 others
                // required because of the requirements on the impl of `Into<usize>` for `data::value::Value`
                // required because of the requirements on the impl of `TryFrom<data::value::Value>` for `usize` [E0277]
            };
        }
        let timestamp = |y, m, d, hh, mm, ss, ms| {
            chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(hh, mm, ss, ms)
        };
        let time = chrono::NaiveTime::from_hms_milli;
        let date = chrono::NaiveDate::from_ymd;
        test!(Value::Bool(true), Ok(1usize));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::I64(1234567890), Ok(1234567890));
        test!(Value::F64(1234567890.0), Ok(1234567890));
        test!(Value::F64(1234567890.1), Ok(1234567890));
        test!(Value::Str("1234567890".to_owned()), Ok(1234567890));
        test!(Value::Decimal(Decimal::new(1234567890, 0)), Ok(1234567890));
        test!(
            Value::Date(date(2021, 11, 20)),
            Err(ValueError::ImpossibleCast.into()) // rustc: the trait bound `Infallible: From<ValueError>` is not satisfied
                                                   // the trait `From<!>` is implemented for `Infallible`
                                                   // required because of the requirements on the impl of `Into<Infallible>` for `ValueError` [E0277]
        );
        // test!(
        //     Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
        //     Err(ValueError::ImpossibleCast.into())
        // );
        // test!(
        //     Value::Time(time(10, 0, 0, 0)),
        //     Err(ValueError::ImpossibleCast.into())
        // );
        // test!(
        //     Value::Interval(I::Month(1)),
        //     Err(ValueError::ImpossibleCast.into())
        // );
        // test!(
        //     Value::Uuid(195965723427462096757863453463987888808),
        //     Err(ValueError::ImpossibleCast.into())
        // );
        // test!(
        //     Value::Map(HashMap::new()),
        //     Err(ValueError::ImpossibleCast.into())
        // );
        // test!(
        //     Value::List(Vec::new()),
        //     Err(ValueError::ImpossibleCast.into())
        // );
        // test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
    }

    #[test]
    fn try_into_naive_date() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<chrono::NaiveDate>, $to);
                assert_eq!(chrono::NaiveDate::try_from($from), $to);
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
                assert_eq!($from.try_into() as Result<chrono::NaiveTime>, $to);
                assert_eq!(chrono::NaiveTime::try_from($from), $to);
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
                assert_eq!($from.try_into() as Result<chrono::NaiveDateTime>, $to);
                assert_eq!(chrono::NaiveDateTime::try_from($from), $to);
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
        );
        assert_eq!(
            I::try_from(&Value::Str("\"+22-10\" YEAR TO MONTH".to_owned())),
            Ok(I::Month(274))
        );
    }

    #[test]
    fn try_into_u128() {
        let uuid = 195965723427462096757863453463987888808;
        assert_eq!((&Value::Uuid(uuid)).try_into() as Result<u128>, Ok(uuid));
        assert_eq!(u128::try_from(&Value::Uuid(uuid)), Ok(uuid));
    }
}
