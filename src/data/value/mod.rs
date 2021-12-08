use {
    super::{Interval, StringExt},
    crate::{ast::DataType, result::Result},
    binary_op::TryBinaryOperator,
    chrono::{NaiveDate, NaiveDateTime, NaiveTime},
    core::ops::Sub,
    serde::{Deserialize, Serialize},
    std::{cmp::Ordering, collections::HashMap, fmt::Debug},
};

mod big_edian;
mod binary_op;
mod date;
mod error;
mod group_key;
mod into;
mod json;
mod literal;
mod selector;
mod unique_key;
mod uuid;

pub use error::ValueError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Bool(bool),
    I8(i8),
    I64(i64),
    F64(f64),
    Str(String),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Time(NaiveTime),
    Interval(Interval),
    Uuid(u128),
    Map(HashMap<String, Value>),
    List(Vec<Value>),
    Null,
}

impl PartialEq<Value> for Value {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Bool(l), Value::Bool(r)) => l == r,
            (Value::I8(l), _) => l == other,
            (Value::I64(l), _) => l == other,
            (Value::F64(l), _) => l == other,
            (Value::Str(l), Value::Str(r)) => l == r,
            (Value::Date(l), Value::Date(r)) => l == r,
            (Value::Date(l), Value::Timestamp(r)) => &l.and_hms(0, 0, 0) == r,
            (Value::Timestamp(l), Value::Date(r)) => l == &r.and_hms(0, 0, 0),
            (Value::Timestamp(l), Value::Timestamp(r)) => l == r,
            (Value::Time(l), Value::Time(r)) => l == r,
            (Value::Interval(l), Value::Interval(r)) => l == r,
            (Value::Uuid(l), Value::Uuid(r)) => l == r,
            (Value::Map(l), Value::Map(r)) => l == r,
            (Value::List(l), Value::List(r)) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Bool(l), Value::Bool(r)) => Some(l.cmp(r)),
            (Value::I8(l), _) => l.partial_cmp(other),
            (Value::I64(l), _) => l.partial_cmp(other),
            (Value::F64(l), _) => l.partial_cmp(other),
            (Value::Str(l), Value::Str(r)) => Some(l.cmp(r)),
            (Value::Date(l), Value::Date(r)) => Some(l.cmp(r)),
            (Value::Date(l), Value::Timestamp(r)) => Some(l.and_hms(0, 0, 0).cmp(r)),
            (Value::Timestamp(l), Value::Date(r)) => Some(l.cmp(&r.and_hms(0, 0, 0))),
            (Value::Timestamp(l), Value::Timestamp(r)) => Some(l.cmp(r)),
            (Value::Time(l), Value::Time(r)) => Some(l.cmp(r)),
            (Value::Interval(l), Value::Interval(r)) => l.partial_cmp(r),
            (Value::Uuid(l), Value::Uuid(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

impl Value {
    pub fn is_zero(&self) -> bool {
        match self {
            Value::I8(v) => *v == 0,
            Value::I64(v) => *v == 0,
            Value::F64(v) => *v == 0.0,
            _ => false,
        }
    }

    pub fn validate_type(&self, data_type: &DataType) -> Result<()> {
        let valid = match self {
            Value::Bool(_) => matches!(data_type, DataType::Boolean),
            Value::I8(_) => matches!(data_type, DataType::Int8),
            Value::I64(_) => matches!(data_type, DataType::Int),
            Value::F64(_) => matches!(data_type, DataType::Float),
            Value::Str(_) => matches!(data_type, DataType::Text),
            Value::Date(_) => matches!(data_type, DataType::Date),
            Value::Timestamp(_) => matches!(data_type, DataType::Timestamp),
            Value::Time(_) => matches!(data_type, DataType::Time),
            Value::Interval(_) => matches!(data_type, DataType::Interval),
            Value::Uuid(_) => matches!(data_type, DataType::Uuid),
            Value::Map(_) => matches!(data_type, DataType::Map),
            Value::List(_) => matches!(data_type, DataType::List),
            Value::Null => true,
        };

        if !valid {
            return Err(ValueError::IncompatibleDataType {
                data_type: data_type.clone(),
                value: self.clone(),
            }
            .into());
        }

        Ok(())
    }

    pub fn validate_null(&self, nullable: bool) -> Result<()> {
        if !nullable && matches!(self, Value::Null) {
            return Err(ValueError::NullValueOnNotNullField.into());
        }

        Ok(())
    }

    pub fn cast(&self, data_type: &DataType) -> Result<Self> {
        match (data_type, self) {
            (DataType::Boolean, Value::Bool(_))
            | (DataType::Int8, Value::I8(_))
            | (DataType::Int, Value::I64(_))
            | (DataType::Float, Value::F64(_))
            | (DataType::Text, Value::Str(_))
            | (DataType::Date, Value::Date(_))
            | (DataType::Timestamp, Value::Timestamp(_))
            | (DataType::Time, Value::Time(_))
            | (DataType::Interval, Value::Interval(_))
            | (DataType::Uuid, Value::Uuid(_)) => Ok(self.clone()),

            (_, Value::Null) => Ok(Value::Null),

            (DataType::Boolean, value) => value.try_into().map(Value::Bool),
            (DataType::Int8, value) => value.try_into().map(Value::I8),
            (DataType::Int, value) => value.try_into().map(Value::I64),
            (DataType::Float, value) => value.try_into().map(Value::F64),
            (DataType::Text, value) => Ok(Value::Str(value.into())),
            (DataType::Date, value) => value.try_into().map(Value::Date),
            (DataType::Time, value) => value.try_into().map(Value::Time),
            (DataType::Timestamp, value) => value.try_into().map(Value::Timestamp),
            (DataType::Interval, value) => value.try_into().map(Value::Interval),
            (DataType::Uuid, value) => value.try_into().map(Value::Uuid),

            _ => Err(ValueError::UnimplementedCast.into()),
        }
    }

    pub fn concat(&self, other: &Value) -> Value {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            _ => Value::Str(String::from(self) + &String::from(other)),
        }
    }

    pub fn add(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I8(a), b) => a.try_add(b),
            (I64(a), b) => a.try_add(b),
            (F64(a), b) => a.try_add(b),
            (Date(a), Time(b)) => Ok(Timestamp(NaiveDateTime::new(*a, *b))),
            (Date(a), Interval(b)) => b.add_date(a).map(Timestamp),
            (Timestamp(a), Interval(b)) => b.add_timestamp(a).map(Timestamp),
            (Time(a), Interval(b)) => b.add_time(a).map(Time),
            (Interval(a), Interval(b)) => a.add(b).map(Interval),
            (Null, I8(_))
            | (Null, I64(_))
            | (Null, F64(_))
            | (Null, Date(_))
            | (Null, Timestamp(_))
            | (Null, Interval(_))
            | (Date(_), Null)
            | (Timestamp(_), Null)
            | (Time(_), Null)
            | (Interval(_), Null)
            | (Null, Null) => Ok(Null),
            _ => Err(ValueError::AddOnNonNumeric(self.clone(), other.clone()).into()),
        }
    }

    pub fn subtract(&self, other: &Value) -> Result<Value> {
        use super::Interval as I;
        use Value::*;

        match (self, other) {
            (I8(a), _) => a.try_subtract(other),
            (I64(a), _) => a.try_subtract(other),
            (F64(a), _) => a.try_subtract(other),
            (Date(a), Date(b)) => Ok(Interval(I::days((*a - *b).num_days() as i32))),
            (Date(a), Interval(b)) => b.subtract_from_date(a).map(Timestamp),
            (Timestamp(a), Interval(b)) => b.subtract_from_timestamp(a).map(Timestamp),
            (Timestamp(a), Timestamp(b)) => a
                .sub(*b)
                .num_microseconds()
                .ok_or_else(|| {
                    ValueError::UnreachableIntegerOverflow(format!("{:?} - {:?}", a, b)).into()
                })
                .map(|v| Interval(I::microseconds(v))),
            (Time(a), Time(b)) => a
                .sub(*b)
                .num_microseconds()
                .ok_or_else(|| {
                    ValueError::UnreachableIntegerOverflow(format!("{:?} - {:?}", a, b)).into()
                })
                .map(|v| Interval(I::microseconds(v))),
            (Time(a), Interval(b)) => b.subtract_from_time(a).map(Time),
            (Interval(a), Interval(b)) => a.subtract(b).map(Interval),
            (Null, I8(_))
            | (Null, I64(_))
            | (Null, F64(_))
            | (Null, Date(_))
            | (Null, Timestamp(_))
            | (Null, Time(_))
            | (Null, Interval(_))
            | (Date(_), Null)
            | (Timestamp(_), Null)
            | (Time(_), Null)
            | (Interval(_), Null)
            | (Null, Null) => Ok(Null),
            _ => Err(ValueError::SubtractOnNonNumeric(self.clone(), other.clone()).into()),
        }
    }

    pub fn multiply(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I8(a), _) => a.try_multiply(other),
            (I64(a), _) => a.try_multiply(other),
            (F64(a), _) => a.try_multiply(other),
            (Interval(a), I8(b)) => Ok(Interval(*a * *b)),
            (Interval(a), I64(b)) => Ok(Interval(*a * *b)),
            (Interval(a), F64(b)) => Ok(Interval(*a * *b)),
            (Null, I8(_))
            | (Null, I64(_))
            | (Null, F64(_))
            | (Null, Interval(_))
            | (Interval(_), Null)
            | (Null, Null) => Ok(Null),
            _ => Err(ValueError::MultiplyOnNonNumeric(self.clone(), other.clone()).into()),
        }
    }

    pub fn divide(&self, other: &Value) -> Result<Value> {
        use Value::*;

        if self.is_zero() {
            return Err(ValueError::DivisorShouldNotBeZero.into());
        }

        match (self, other) {
            (I8(a), _) => a.try_divide(other),
            (I64(a), _) => a.try_divide(other),
            (F64(a), _) => a.try_divide(other),
            (Interval(a), I8(b)) => Ok(Interval(*a / *b)),
            (Interval(a), I64(b)) => Ok(Interval(*a / *b)),
            (Interval(a), F64(b)) => Ok(Interval(*a / *b)),
            (Null, I8(_))
            | (Null, I64(_))
            | (Null, F64(_))
            | (Interval(_), Null)
            | (Null, Null) => Ok(Null),
            _ => Err(ValueError::DivideOnNonNumeric(self.clone(), other.clone()).into()),
        }
    }

    pub fn modulo(&self, other: &Value) -> Result<Value> {
        use Value::*;

        if self.is_zero() {
            return Err(ValueError::DivisorShouldNotBeZero.into());
        }

        match (self, other) {
            (I8(a), _) => a.try_modulo(other),
            (I64(a), _) => a.try_modulo(other),
            (F64(a), _) => a.try_modulo(other),
            (Null, I8(_)) | (Null, I64(_)) | (Null, F64(_)) | (Null, Null) => Ok(Null),
            _ => Err(ValueError::ModuloOnNonNumeric(self.clone(), other.clone()).into()),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn unary_plus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I8(_) | I64(_) | F64(_) | Interval(_) => Ok(self.clone()),
            Null => Ok(Null),
            _ => Err(ValueError::UnaryPlusOnNonNumeric.into()),
        }
    }

    pub fn unary_minus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I8(a) => Ok(I8(-a)),
            I64(a) => Ok(I64(-a)),
            F64(a) => Ok(F64(-a)),
            Interval(a) => Ok(Interval(a.unary_minus())),
            Null => Ok(Null),
            _ => Err(ValueError::UnaryMinusOnNonNumeric.into()),
        }
    }

    pub fn like(&self, other: &Value, case_sensitive: bool) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (Str(a), Str(b)) => a.like(b, case_sensitive).map(Bool),
            _ => match case_sensitive {
                true => Err(ValueError::LikeOnNonString(self.clone(), other.clone()).into()),
                false => Err(ValueError::ILikeOnNonString(self.clone(), other.clone()).into()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Interval, Value::*},
        crate::data::value::uuid::parse_uuid,
    };

    #[allow(clippy::eq_op)]
    #[test]
    fn eq() {
        use super::Interval;
        use chrono::{NaiveDateTime, NaiveTime};

        assert_ne!(Null, Null);
        assert_eq!(Bool(true), Bool(true));
        assert_eq!(I8(1), I8(1));
        assert_eq!(I64(1), I64(1));
        assert_eq!(I64(1), F64(1.0));
        assert_eq!(F64(1.0), I64(1));
        assert_eq!(F64(6.11), F64(6.11));
        assert_eq!(Str("Glue".to_owned()), Str("Glue".to_owned()));
        assert_eq!(Interval::Month(1), Interval::Month(1));
        assert_eq!(
            Time(NaiveTime::from_hms(12, 30, 11)),
            Time(NaiveTime::from_hms(12, 30, 11))
        );

        let date = Date("2020-05-01".parse().unwrap());
        let timestamp = Timestamp("2020-05-01T00:00:00".parse::<NaiveDateTime>().unwrap());

        assert_eq!(date, timestamp);
        assert_eq!(timestamp, date);

        assert_eq!(
            Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()),
            Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap())
        );
    }

    #[test]
    fn cmp() {
        use chrono::{NaiveDate, NaiveTime};
        use std::cmp::Ordering;

        let date = Date(NaiveDate::from_ymd(2020, 5, 1));
        let timestamp = Timestamp(NaiveDate::from_ymd(2020, 3, 1).and_hms(0, 0, 0));

        assert_eq!(date.partial_cmp(&timestamp), Some(Ordering::Greater));
        assert_eq!(timestamp.partial_cmp(&date), Some(Ordering::Less));

        assert_eq!(
            Time(NaiveTime::from_hms(23, 0, 1)).partial_cmp(&Time(NaiveTime::from_hms(10, 59, 59))),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Interval::Month(1).partial_cmp(&Interval::Month(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Interval::Microsecond(1).partial_cmp(&Interval::Month(2)),
            None
        );
    }

    #[test]
    fn arithmetic() {
        use chrono::{NaiveDate, NaiveTime};

        macro_rules! test {
            ($op: ident $a: expr, $b: expr => $c: expr) => {
                assert_eq!($a.$op(&$b), Ok($c));
            };
        }

        macro_rules! mon {
            ($n: expr) => {
                Interval(Interval::Month($n))
            };
        }

        let time = |h, m, s| NaiveTime::from_hms(h, m, s);
        let date = |y, m, d| NaiveDate::from_ymd(y, m, d);

        test!(add I8(1),    I8(2)    => I8(3));
        test!(add I8(1),    I64(2)   => I64(3));
        test!(add I8(1),    F64(2.0) => F64(3.0));

        test!(add I64(1),   I64(2)   => I64(3));
        test!(add I64(1),   I8(2)    => I64(3));
        test!(add I64(1),   F64(2.0) => F64(3.0));

        test!(add F64(1.0), F64(2.0) => F64(3.0));
        test!(add F64(1.0), I8(2)    => F64(3.0));
        test!(add F64(1.0), I64(2)   => F64(3.0));

        test!(add
            Date(date(2021, 11, 11)),
            mon!(14)
            =>
            Timestamp(date(2023, 1, 11).and_hms(0, 0, 0))
        );
        test!(add
            Date(date(2021, 5, 7)),
            Time(time(12, 0, 0))
            =>
            Timestamp(date(2021, 5, 7).and_hms(12, 0, 0))
        );
        test!(add
            Timestamp(date(2021, 11, 11).and_hms(0, 0, 0)),
            mon!(14)
            =>
            Timestamp(date(2023, 1, 11).and_hms(0, 0, 0))
        );
        test!(add
            Time(time(1, 4, 6)),
            Interval(Interval::hours(20))
            =>
            Time(time(21, 4, 6))
        );
        test!(add
            Time(time(23, 10, 0)),
            Interval(Interval::hours(5))
            =>
            Time(time(4, 10, 0))
        );
        test!(add mon!(1),  mon!(2)  => mon!(3));

        test!(subtract I8(3),    I8(2)    => I8(1));
        test!(subtract I8(3),    I64(2)   => I64(1));
        test!(subtract I8(3),    F64(2.0) => F64(1.0));

        test!(subtract I64(3),   I64(2)   => I64(1));
        test!(subtract I64(3),   I8(2)    => I64(1));
        test!(subtract I64(3),   F64(2.0) => F64(1.0));

        test!(subtract F64(3.0), F64(2.0) => F64(1.0));
        test!(subtract F64(3.0), I8(2)    => F64(1.0));
        test!(subtract F64(3.0), I64(2)   => F64(1.0));

        test!(subtract
            Date(NaiveDate::from_ymd(2021, 11, 11)),
            Date(NaiveDate::from_ymd(2021, 6, 11))
            =>
            Interval(Interval::days(153))
        );
        test!(subtract
            Date(NaiveDate::from_ymd(2021, 1, 1)),
            Interval(Interval::days(365))
            =>
            Timestamp(NaiveDate::from_ymd(2020, 1, 2).and_hms(0, 0, 0))
        );
        test!(subtract
            Timestamp(NaiveDate::from_ymd(2021, 1, 1).and_hms(15, 0, 0)),
            Timestamp(NaiveDate::from_ymd(2021, 1, 1).and_hms(12, 0, 0))
            =>
            Interval(Interval::hours(3))
        );
        test!(subtract
            Timestamp(NaiveDate::from_ymd(2021, 1, 1).and_hms(0, 3, 0)),
            Interval(Interval::days(365))
            =>
            Timestamp(NaiveDate::from_ymd(2020, 1, 2).and_hms(0, 3, 0))
        );
        test!(subtract
            Time(time(1, 4, 6)),
            Interval(Interval::hours(20))
            =>
            Time(time(5, 4, 6))
        );
        test!(subtract
            Time(time(23, 10, 0)),
            Interval(Interval::hours(5))
            =>
            Time(time(18, 10, 0))
        );
        test!(subtract mon!(1),  mon!(2)  => mon!(-1));

        test!(multiply I8(3),    I8(2)    => I8(6));
        test!(multiply I8(3),    I64(2)   => I64(6));
        test!(multiply I8(3),    F64(2.0) => F64(6.0));

        test!(multiply I64(3),   I64(2)   => I64(6));
        test!(multiply I64(3),   I8(2)    => I64(6));
        test!(multiply I64(3),   F64(2.0) => F64(6.0));

        test!(multiply F64(3.0), F64(2.0) => F64(6.0));
        test!(multiply F64(3.0), I8(2)    => F64(6.0));
        test!(multiply F64(3.0), I64(2)   => F64(6.0));

        test!(multiply I8(3),    mon!(3)  => mon!(9));
        test!(multiply I64(3),   mon!(3)  => mon!(9));
        test!(multiply F64(3.0), mon!(3)  => mon!(9));
        test!(multiply mon!(3),  I8(2)   => mon!(6));
        test!(multiply mon!(3),  I64(2)   => mon!(6));
        test!(multiply mon!(3),  F64(2.0) => mon!(6));

        test!(divide I8(6),    I8(2)    => I8(3));
        test!(divide I8(6),    I8(2)    => I64(3));
        test!(divide I8(6),    F64(2.0) => F64(3.0));

        test!(divide I64(6),   I64(2)   => I64(3));
        test!(divide I64(6),   I8(2)    => I64(3));
        test!(divide I64(6),   F64(2.0) => F64(3.0));

        test!(divide F64(6.0), I8(2)    => F64(3.0));
        test!(divide F64(6.0), I64(2)   => F64(3.0));
        test!(divide F64(6.0), F64(2.0) => F64(3.0));

        test!(divide mon!(6),  I8(2)    => mon!(3));
        test!(divide mon!(6),  I64(2)   => mon!(3));
        test!(divide mon!(6),  F64(2.0) => mon!(3));

        test!(modulo I64(6),   I64(2)   => I64(0));
        test!(modulo I64(6),   F64(2.0) => F64(0.0));
        test!(modulo F64(6.0), I64(2)   => F64(0.0));
        test!(modulo F64(6.0), F64(2.0) => F64(0.0));

        macro_rules! null_test {
            ($op: ident $a: expr, $b: expr) => {
                matches!($a.$op(&$b), Ok(Null));
            };
        }

        let date = || Date(NaiveDate::from_ymd(1989, 3, 1));
        let time = || Time(NaiveTime::from_hms(6, 1, 1));
        let ts = || Timestamp(NaiveDate::from_ymd(1989, 1, 1).and_hms(0, 0, 0));

        null_test!(add      I8(1),    Null);
        null_test!(add      I64(1),   Null);
        null_test!(add      F64(1.0), Null);
        null_test!(add      date(),   Null);
        null_test!(add      ts(),     Null);
        null_test!(add      time(),   Null);
        null_test!(add      mon!(1),  Null);
        null_test!(subtract I8(1),    Null);
        null_test!(subtract I64(1),   Null);
        null_test!(subtract F64(1.0), Null);
        null_test!(subtract date(),   Null);
        null_test!(subtract ts(),     Null);
        null_test!(subtract time(),   Null);
        null_test!(subtract mon!(1),  Null);
        null_test!(multiply I8(1),    Null);
        null_test!(multiply I64(1),   Null);
        null_test!(multiply F64(1.0), Null);
        null_test!(multiply mon!(1),  Null);
        null_test!(divide   I8(1),    Null);
        null_test!(divide   I64(1),   Null);
        null_test!(divide   F64(1.0), Null);
        null_test!(divide   mon!(1),  Null);
        null_test!(modulo   I8(1),    Null);
        null_test!(modulo   I64(1),   Null);
        null_test!(modulo   F64(1.0), Null);

        null_test!(add      Null, I8(1));
        null_test!(add      Null, I64(1));
        null_test!(add      Null, F64(1.0));
        null_test!(add      Null, mon!(1));
        null_test!(add      Null, date());
        null_test!(add      Null, ts());
        null_test!(subtract Null, I8(1));
        null_test!(subtract Null, I64(1));
        null_test!(subtract Null, F64(1.0));
        null_test!(subtract Null, date());
        null_test!(subtract Null, ts());
        null_test!(subtract Null, time());
        null_test!(subtract Null, mon!(1));
        null_test!(multiply Null, I8(1));
        null_test!(multiply Null, I64(1));
        null_test!(multiply Null, F64(1.0));
        null_test!(divide   Null, I8(1));
        null_test!(divide   Null, I64(1));
        null_test!(divide   Null, F64(1.0));
        null_test!(modulo   Null, I8(1));
        null_test!(modulo   Null, I64(1));
        null_test!(modulo   Null, F64(1.0));

        null_test!(add      Null, Null);
        null_test!(subtract Null, Null);
        null_test!(multiply Null, Null);
        null_test!(divide   Null, Null);
        null_test!(modulo   Null, Null);
    }

    #[test]
    fn cast() {
        use {
            crate::{ast::DataType::*, prelude::Value},
            chrono::{NaiveDate, NaiveTime},
        };

        macro_rules! cast {
            ($input: expr => $data_type: expr, $expected: expr) => {
                let found = $input.cast(&$data_type).unwrap();

                match ($expected, found) {
                    (Null, Null) => {}
                    (expected, found) => {
                        assert_eq!(expected, found);
                    }
                }
            };
        }

        // Same as
        cast!(Bool(true)            => Boolean      , Bool(true));
        cast!(Str("a".to_owned())   => Text         , Str("a".to_owned()));
        cast!(I8(1)                 => Int8          , I8(1));
        cast!(I64(1)                => Int          , I64(1));
        cast!(F64(1.0)              => Float        , F64(1.0));
        cast!(Value::Uuid(123)      => Uuid         , Value::Uuid(123));

        // Boolean
        cast!(Str("TRUE".to_owned())    => Boolean, Bool(true));
        cast!(Str("FALSE".to_owned())   => Boolean, Bool(false));
        cast!(I8(1)                     => Boolean, Bool(true));
        cast!(I8(0)                     => Boolean, Bool(false));
        cast!(I64(1)                    => Boolean, Bool(true));
        cast!(I64(0)                    => Boolean, Bool(false));
        cast!(F64(1.0)                  => Boolean, Bool(true));
        cast!(F64(0.0)                  => Boolean, Bool(false));
        cast!(Null                      => Boolean, Null);

        // Integer
        cast!(Bool(true)            => Int8, I8(1));
        cast!(Bool(false)           => Int8, I8(0));
        cast!(F64(1.1)              => Int8, I8(1));
        cast!(Str("11".to_owned())  => Int8, I8(11));
        cast!(Null                  => Int8, Null);

        cast!(Bool(true)            => Int, I64(1));
        cast!(Bool(false)           => Int, I64(0));
        cast!(F64(1.1)              => Int, I64(1));
        cast!(Str("11".to_owned())  => Int, I64(11));
        cast!(Null                  => Int, Null);

        // Float
        cast!(Bool(true)            => Float, F64(1.0));
        cast!(Bool(false)           => Float, F64(0.0));
        cast!(I8(1)                 => Float, F64(1.0));
        cast!(I64(1)                => Float, F64(1.0));
        cast!(Str("11".to_owned())  => Float, F64(11.0));
        cast!(Null                  => Float, Null);

        // Text
        cast!(Bool(true)    => Text, Str("TRUE".to_owned()));
        cast!(Bool(false)   => Text, Str("FALSE".to_owned()));
        cast!(I8(11)        => Text, Str("11".to_owned()));
        cast!(I64(11)       => Text, Str("11".to_owned()));
        cast!(F64(1.0)      => Text, Str("1".to_owned()));

        let date = Value::Date(NaiveDate::from_ymd(2021, 5, 1));
        cast!(date          => Text, Str("2021-05-01".to_owned()));

        let timestamp = Value::Timestamp(NaiveDate::from_ymd(2021, 5, 1).and_hms(12, 34, 50));
        cast!(timestamp     => Text, Str("2021-05-01 12:34:50".to_owned()));
        cast!(Null          => Text, Null);

        // Date
        let date = Value::Date(NaiveDate::from_ymd(2021, 5, 1));
        let timestamp = Value::Timestamp(NaiveDate::from_ymd(2021, 5, 1).and_hms(12, 34, 50));

        cast!(Str("2021-05-01".to_owned()) => Date, date.to_owned());
        cast!(timestamp                    => Date, date);
        cast!(Null                         => Date, Null);

        // Time
        cast!(Str("08:05:30".to_owned()) => Time, Value::Time(NaiveTime::from_hms(8, 5, 30)));
        cast!(Null                       => Time, Null);

        // Timestamp
        cast!(Value::Date(NaiveDate::from_ymd(2021, 5, 1)) => Timestamp, Value::Timestamp(NaiveDate::from_ymd(2021, 5, 1).and_hms(0, 0, 0)));
        cast!(Str("2021-05-01 08:05:30".to_owned())        => Timestamp, Value::Timestamp(NaiveDate::from_ymd(2021, 5, 1).and_hms(8, 5, 30)));
        cast!(Null                                         => Timestamp, Null);
    }

    #[test]
    fn concat() {
        let a = Str("A".to_owned());

        assert_eq!(a.concat(&Str("B".to_owned())), Str("AB".to_owned()));
        assert_eq!(a.concat(&Bool(true)), Str("ATRUE".to_owned()));
        assert_eq!(a.concat(&I64(1)), Str("A1".to_owned()));
        assert_eq!(a.concat(&F64(1.0)), Str("A1".to_owned()));
        assert_eq!(I64(2).concat(&I64(1)), Str("21".to_owned()));
        matches!(a.concat(&Null), Null);
    }

    #[test]
    fn validate_type() {
        use {
            super::{Value, ValueError},
            crate::{ast::DataType as D, data::Interval as I},
            chrono::{NaiveDate, NaiveTime},
        };

        let date = Date(NaiveDate::from_ymd(2021, 5, 1));
        let timestamp = Timestamp(NaiveDate::from_ymd(2021, 5, 1).and_hms(12, 34, 50));
        let time = Time(NaiveTime::from_hms(12, 30, 11));
        let interval = Interval(I::hours(5));
        let uuid = Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap());
        let map = Value::parse_json_map(r#"{ "a": 10 }"#).unwrap();
        let list = Value::parse_json_list(r#"[ true ]"#).unwrap();

        assert!(Bool(true).validate_type(&D::Boolean).is_ok());
        assert!(Bool(true).validate_type(&D::Int).is_err());
        assert!(I8(1).validate_type(&D::Int8).is_ok());
        assert!(I8(1).validate_type(&D::Text).is_err());
        assert!(I64(1).validate_type(&D::Int).is_ok());
        assert!(I64(1).validate_type(&D::Text).is_err());
        assert!(F64(1.0).validate_type(&D::Float).is_ok());
        assert!(F64(1.0).validate_type(&D::Int).is_err());
        assert!(Str("a".to_owned()).validate_type(&D::Text).is_ok());
        assert!(Str("a".to_owned()).validate_type(&D::Int).is_err());
        assert!(date.validate_type(&D::Date).is_ok());
        assert!(date.validate_type(&D::Text).is_err());
        assert!(timestamp.validate_type(&D::Timestamp).is_ok());
        assert!(timestamp.validate_type(&D::Boolean).is_err());
        assert!(time.validate_type(&D::Time).is_ok());
        assert!(time.validate_type(&D::Date).is_err());
        assert!(interval.validate_type(&D::Interval).is_ok());
        assert!(interval.validate_type(&D::Date).is_err());
        assert!(uuid.validate_type(&D::Uuid).is_ok());
        assert!(uuid.validate_type(&D::Boolean).is_err());
        assert!(map.validate_type(&D::Map).is_ok());
        assert!(map.validate_type(&D::Int).is_err());
        assert!(list.validate_type(&D::List).is_ok());
        assert!(list.validate_type(&D::Int).is_err());
        assert!(Null.validate_type(&D::Time).is_ok());
        assert!(Null.validate_type(&D::Boolean).is_ok());

        assert_eq!(
            Bool(true).validate_type(&D::Text),
            Err(ValueError::IncompatibleDataType {
                data_type: D::Text,
                value: Bool(true),
            }
            .into()),
        );
    }
}
