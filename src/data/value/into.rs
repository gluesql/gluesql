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
    uuid::Uuid,
};

impl From<&Value> for String {
    fn from(v: &Value) -> Self {
        match v {
            Value::Str(value) => value.to_string(),
            Value::Bool(value) => (if *value { "TRUE" } else { "FALSE" }).to_string(),
            Value::I64(value) => value.to_string(),
            Value::F64(value) => value.to_string(),
            Value::Date(value) => value.to_string(),
            Value::Timestamp(value) => value.to_string(),
            Value::Time(value) => value.to_string(),
            Value::Interval(value) => String::from(value),
            Value::Uuid(value) => Uuid::from_u128(*value).to_string(),
            Value::Map(_) => "[MAP]".to_owned(),
            Value::List(_) => "[LIST]".to_owned(),
            Value::Null => String::from("NULL"),
        }
    }
}

impl From<Value> for String {
    fn from(v: Value) -> Self {
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
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryInto<bool> for Value {
    type Error = Error;

    fn try_into(self) -> Result<bool> {
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
            Value::I64(value) => *value,
            Value::F64(value) => value.trunc() as i64,
            Value::Str(value) => value
                .parse::<i64>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
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
            Value::I64(value) => (*value as f64).trunc(),
            Value::F64(value) => *value,
            Value::Str(value) => value
                .parse::<f64>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
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
        super::{Interval, Value::*},
        crate::data::{
            value::uuid::parse_uuid,
            value::HashMap,
            value::Value,
            ValueError,
        },
        crate::result::Result,
        chrono::{NaiveDateTime, NaiveTime},
    };

    #[test]
    fn into_string() {
        macro_rules! test (
            ($value: expr, $string: expr) => {
                assert_eq!(String::from($value), String::from($string));
            }
        );

        test!(&Str("Glue".to_owned()), "Glue");
        test!(Str("Glue".to_owned()), "Glue");
        test!(Bool(true), "TRUE");
        test!(I64(1), "1");
        test!(F64(1.0), "1");
        test!(Date("2021-12-25".parse().unwrap()), "2021-12-25");
        test!(
            Timestamp("2021-12-25T00:00:00".parse::<NaiveDateTime>().unwrap()),
            "2021-12-25 00:00:00"
        );
        test!(Time(NaiveTime::from_hms(12, 30, 11)), "12:30:11");
        test!(Interval::Month(1), r#""1" MONTH"#);
        test!(
            Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()),
            "936da01f-9abd-4d9d-80c7-02af85c822a8"
        );
        let m: HashMap<String, Value> =
            [("key1".to_owned(), I64(10)), ("key2".to_owned(), I64(20))].into();
        test!(Map(m), "[MAP]");
        test!(List(vec![I64(1), I64(2), I64(3)]), "[LIST]");
        test!(Null, "NULL");
    }

    #[test]
    fn into_bool() {        
        macro_rules! test (
            ($value: expr, $expected: expr) => {
                assert_eq!($value.try_into(), $expected)
            }
        );

        test!(Bool(true), Ok(true));
        test!(I64(1), Ok(true));
        test!(I64(0), Ok(false));
        test!(F64(1.0), Ok(true));
        test!(F64(0.0), Ok(false));
        test!(Str("TRUE".to_owned()), Ok(true));
        test!(Str("FALSE".to_owned()), Ok(false));

        macro_rules! errtest (
            ($value: expr) => {
                let v: Result<bool> = $value.try_into();
                assert_eq!(v, Err(ValueError::ImpossibleCast.into()));
            }
        );

        errtest!(I64(5));
        errtest!(F64(5.0));
        errtest!(Str("Glue".to_owned()));
        errtest!(Date("2021-12-25".parse().unwrap()));
        errtest!(Timestamp("2021-12-25T00:00:00".parse::<NaiveDateTime>().unwrap()));
        errtest!(Time(NaiveTime::from_hms(12, 30, 11)));
        errtest!(Interval(Interval::hours(5)));
        errtest!(Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()));
        let m: HashMap<String, Value> =
            [("key1".to_owned(), I64(10)), ("key2".to_owned(), I64(20))].into();
        errtest!(Map(m));
        errtest!(List(vec![I64(1), I64(2), I64(3)]));
        errtest!(Null);
    }

    #[test]
    fn into_i64() {
        macro_rules! test (
            ($value: expr, $expected: expr) => {
                assert_eq!($value.try_into(), $expected)
            }
        );

        test!(Bool(true), Ok(1));
        test!(Bool(false), Ok(0));
        test!(I64(1), Ok(1));
        test!(F64(1.7), Ok(1));
        test!(Str("1".to_owned()), Ok(1));

        macro_rules! test_err (
            ($value: expr) => {
                let v: Result<i64> = $value.try_into();
                assert_eq!(v, Err(ValueError::ImpossibleCast.into()));
            }
        );

        test_err!(Str("Glue".to_owned()));
        test_err!(Date("2021-12-25".parse().unwrap()));
        test_err!(Timestamp("2021-12-25T00:00:00".parse::<NaiveDateTime>().unwrap()));
        test_err!(Time(NaiveTime::from_hms(12, 30, 11)));
        test_err!(Interval(Interval::hours(5)));
        test_err!(Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()));
        let m: HashMap<String, Value> =
            [("key1".to_owned(), I64(10)), ("key2".to_owned(), I64(20))].into();
        test_err!(Map(m));
        test_err!(List(vec![I64(1), I64(2), I64(3)]));
        test_err!(Null);
    }

    #[test]
    fn into_f64() {
        macro_rules! test (
            ($value: expr, $expected: expr) => {
                assert_eq!((&$value).try_into(), $expected)
            }
        );

        test!(Bool(true), Ok(1.0));
        test!(Bool(false), Ok(0.0));
        test!(I64(1), Ok(1.0));
        test!(F64(1.7), Ok(1.7));
        test!(Str("1.0".to_owned()), Ok(1.0));

        macro_rules! test_err (
            ($value: expr) => {
                let v: Result<f64> = (&$value).try_into();
                assert_eq!(v, Err(ValueError::ImpossibleCast.into()));
            }
        );

        test_err!(Str("Glue".to_owned()));
        test_err!(Date("2021-12-25".parse().unwrap()));
        test_err!(Timestamp("2021-12-25T00:00:00".parse::<NaiveDateTime>().unwrap()));
        test_err!(Time(NaiveTime::from_hms(12, 30, 11)));
        test_err!(Interval(Interval::hours(5)));
        test_err!(Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()));
        let m: HashMap<String, Value> =
            [("key1".to_owned(), I64(10)), ("key2".to_owned(), I64(20))].into();
        test_err!(Map(m));
        test_err!(List(vec![I64(1), I64(2), I64(3)]));
        test_err!(Null);
    }
}
