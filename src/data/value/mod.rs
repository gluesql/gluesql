use {
    super::Literal,
    crate::result::Result,
    chrono::{NaiveDate, NaiveDateTime},
    serde::{Deserialize, Serialize},
    sqlparser::ast::{DataType, Expr},
    std::{
        cmp::Ordering,
        convert::{TryFrom, TryInto},
        fmt::Debug,
    },
};

mod error;
mod group_key;
mod into;
mod literal;
mod unique_key;

pub use {error::ValueError, literal::TryFromLiteral};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Null,
}

impl PartialEq<Value> for Value {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Bool(l), Value::Bool(r)) => l == r,
            (Value::I64(l), Value::I64(r)) => l == r,
            (Value::F64(l), Value::F64(r)) => l == r,
            (Value::Str(l), Value::Str(r)) => l == r,
            (Value::Date(l), Value::Date(r)) => l == r,
            (Value::Date(l), Value::Timestamp(r)) => &l.and_hms(0, 0, 0) == r,
            (Value::Timestamp(l), Value::Date(r)) => l == &r.and_hms(0, 0, 0),
            (Value::Timestamp(l), Value::Timestamp(r)) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Bool(l), Value::Bool(r)) => Some(l.cmp(r)),
            (Value::I64(l), Value::I64(r)) => Some(l.cmp(r)),
            (Value::I64(l), Value::F64(r)) => (*l as f64).partial_cmp(r),
            (Value::F64(l), Value::I64(r)) => l.partial_cmp(&(*r as f64)),
            (Value::F64(l), Value::F64(r)) => l.partial_cmp(r),
            (Value::Str(l), Value::Str(r)) => Some(l.cmp(r)),
            (Value::Date(l), Value::Date(r)) => l.partial_cmp(r),
            (Value::Date(l), Value::Timestamp(r)) => l.and_hms(0, 0, 0).partial_cmp(r),
            (Value::Timestamp(l), Value::Date(r)) => l.partial_cmp(&r.and_hms(0, 0, 0)),
            (Value::Timestamp(l), Value::Timestamp(r)) => l.partial_cmp(r),
            _ => None,
        }
    }
}

trait BoolToValue: Sized {
    fn into_value(self, v1: Value, v2: Value) -> Value;
}

impl BoolToValue for bool {
    #[inline]
    fn into_value(self, v1: Value, v2: Value) -> Value {
        if self {
            v1
        } else {
            v2
        }
    }
}

impl Value {
    pub fn from_expr(data_type: &DataType, nullable: bool, expr: &Expr) -> Result<Self> {
        let literal = Literal::try_from(expr)?;
        let value = Value::try_from_literal(&data_type, &literal)?;

        value.validate_null(nullable)?;

        Ok(value)
    }

    pub fn validate_type(&self, data_type: &DataType) -> Result<()> {
        let valid = matches!(
            (data_type, self),
            (DataType::Boolean, Value::Bool(_))
                | (DataType::Int, Value::I64(_))
                | (DataType::Float(_), Value::F64(_))
                | (DataType::Text, Value::Str(_))
                | (DataType::Date, Value::Date(_))
                | (DataType::Timestamp, Value::Timestamp(_))
                | (DataType::Boolean, Value::Null)
                | (DataType::Int, Value::Null)
                | (DataType::Float(_), Value::Null)
                | (DataType::Text, Value::Null)
                | (DataType::Date, Value::Null)
                | (DataType::Timestamp, Value::Null)
        );

        if !valid {
            return Err(ValueError::IncompatibleDataType {
                data_type: data_type.to_string(),
                value: format!("{:?}", self),
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
            | (DataType::Int, Value::I64(_))
            | (DataType::Float(_), Value::F64(_))
            | (DataType::Text, Value::Str(_))
            | (DataType::Date, Value::Date(_))
            | (DataType::Timestamp, Value::Timestamp(_)) => Ok(self.clone()),

            (_, Value::Null) => Ok(Value::Null),

            (DataType::Boolean, value) => value.try_into().map(Value::Bool),
            (DataType::Int, value) => value.try_into().map(Value::I64),
            (DataType::Float(_), value) => value.try_into().map(Value::F64),
            (DataType::Text, value) => Ok(Value::Str(value.into())),
            (DataType::Date, value) => value.try_into().map(Value::Date),
            (DataType::Timestamp, value) => value.try_into().map(Value::Timestamp),

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
            (I64(a), I64(b)) => Ok(I64(a + b)),
            (F64(a), F64(b)) => Ok(F64(a + b)),
            (I64(a), F64(b)) | (F64(b), I64(a)) => Ok(F64(*a as f64 + b)),
            (Null, I64(_)) | (Null, F64(_)) | (I64(_), Null) | (F64(_), Null) | (Null, Null) => {
                Ok(Null)
            }
            _ => Err(
                ValueError::AddOnNonNumeric(format!("{:?}", self), format!("{:?}", other)).into(),
            ),
        }
    }

    pub fn subtract(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a - b)),
            (I64(a), F64(b)) => Ok(F64(*a as f64 - b)),
            (F64(a), I64(b)) => Ok(F64(a - *b as f64)),
            (F64(a), F64(b)) => Ok(F64(a - b)),
            (Null, I64(_)) | (Null, F64(_)) | (I64(_), Null) | (F64(_), Null) | (Null, Null) => {
                Ok(Null)
            }
            _ => Err(ValueError::SubtractOnNonNumeric(
                format!("{:?}", self),
                format!("{:?}", other),
            )
            .into()),
        }
    }

    pub fn multiply(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a * b)),
            (F64(a), F64(b)) => Ok(F64(a * b)),
            (I64(a), F64(b)) | (F64(b), I64(a)) => Ok(F64(*a as f64 * b)),
            (Null, I64(_)) | (Null, F64(_)) | (I64(_), Null) | (F64(_), Null) | (Null, Null) => {
                Ok(Null)
            }
            _ => Err(ValueError::MultiplyOnNonNumeric(
                format!("{:?}", self),
                format!("{:?}", other),
            )
            .into()),
        }
    }

    pub fn divide(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a / b)),
            (I64(a), F64(b)) => Ok(F64(*a as f64 / b)),
            (F64(a), I64(b)) => Ok(F64(a / *b as f64)),
            (F64(a), F64(b)) => Ok(F64(a / b)),
            (Null, I64(_)) | (Null, F64(_)) | (I64(_), Null) | (F64(_), Null) | (Null, Null) => {
                Ok(Null)
            }
            _ => Err(
                ValueError::DivideOnNonNumeric(format!("{:?}", self), format!("{:?}", other))
                    .into(),
            ),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn unary_plus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I64(_) | F64(_) => Ok(self.clone()),
            Null => Ok(Null),
            _ => Err(ValueError::UnaryPlusOnNonNumeric.into()),
        }
    }

    pub fn unary_minus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I64(a) => Ok(I64(-a)),
            F64(a) => Ok(F64(-a)),
            Null => Ok(Null),
            _ => Err(ValueError::UnaryMinusOnNonNumeric.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Value::*;

    #[test]
    fn eq() {
        use chrono::NaiveDateTime;

        assert_ne!(Null, Null);
        assert_eq!(Bool(true), Bool(true));
        assert_eq!(I64(1), I64(1));
        assert_eq!(F64(6.11), F64(6.11));
        assert_eq!(Str("Glue".to_owned()), Str("Glue".to_owned()));

        let date = Date("2020-05-01".parse().unwrap());
        let timestamp = Timestamp("2020-05-01T00:00:00".parse::<NaiveDateTime>().unwrap());

        assert_eq!(date, timestamp);
        assert_eq!(timestamp, date);
    }

    #[test]
    fn cmp() {
        use chrono::NaiveDate;
        use std::cmp::Ordering;

        let date = Date(NaiveDate::from_ymd(2020, 5, 1));
        let timestamp = Timestamp(NaiveDate::from_ymd(2020, 3, 1).and_hms(0, 0, 0));

        assert_eq!(date.partial_cmp(&timestamp), Some(Ordering::Greater));
        assert_eq!(timestamp.partial_cmp(&date), Some(Ordering::Less));
    }

    #[test]
    fn arithmetic() {
        macro_rules! test {
            ($op: ident $a: expr, $b: expr => $c: expr) => {
                assert_eq!($a.$op(&$b), Ok($c));
            };
        }

        test!(add I64(1),   I64(2)   => I64(3));
        test!(add I64(1),   F64(2.0) => F64(3.0));
        test!(add F64(1.0), I64(2)   => F64(3.0));
        test!(add F64(1.0), F64(2.0) => F64(3.0));

        test!(subtract I64(3),   I64(2)   => I64(1));
        test!(subtract I64(3),   F64(2.0) => F64(1.0));
        test!(subtract F64(3.0), I64(2)   => F64(1.0));
        test!(subtract F64(3.0), F64(2.0) => F64(1.0));

        test!(multiply I64(3),   I64(2)   => I64(6));
        test!(multiply I64(3),   F64(2.0) => F64(6.0));
        test!(multiply F64(3.0), I64(2)   => F64(6.0));
        test!(multiply F64(3.0), F64(2.0) => F64(6.0));

        test!(divide I64(6),   I64(2)   => I64(3));
        test!(divide I64(6),   F64(2.0) => F64(3.0));
        test!(divide F64(6.0), I64(2)   => F64(3.0));
        test!(divide F64(6.0), F64(2.0) => F64(3.0));

        macro_rules! null_test {
            ($op: ident $a: expr, $b: expr) => {
                matches!($a.$op(&$b), Ok(Null));
            };
        }

        null_test!(add      I64(1),   Null);
        null_test!(add      F64(1.0), Null);
        null_test!(subtract I64(1),   Null);
        null_test!(subtract F64(1.0), Null);
        null_test!(multiply I64(1),   Null);
        null_test!(multiply F64(1.0), Null);
        null_test!(divide   I64(1),   Null);
        null_test!(divide   F64(1.0), Null);

        null_test!(add      Null, I64(1));
        null_test!(add      Null, F64(1.0));
        null_test!(subtract Null, I64(1));
        null_test!(subtract Null, F64(1.0));
        null_test!(multiply Null, I64(1));
        null_test!(multiply Null, F64(1.0));
        null_test!(divide   Null, I64(1));
        null_test!(divide   Null, F64(1.0));

        null_test!(add      Null, Null);
        null_test!(subtract Null, Null);
        null_test!(multiply Null, Null);
        null_test!(divide   Null, Null);
    }

    #[test]
    fn cast() {
        use {crate::Value, chrono::NaiveDate, sqlparser::ast::DataType::*};

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
        cast!(I64(1)                => Int          , I64(1));
        cast!(F64(1.0)              => Float(None)  , F64(1.0));

        // Boolean
        cast!(Str("TRUE".to_owned())    => Boolean, Bool(true));
        cast!(Str("FALSE".to_owned())   => Boolean, Bool(false));
        cast!(I64(1)                    => Boolean, Bool(true));
        cast!(I64(0)                    => Boolean, Bool(false));
        cast!(F64(1.0)                  => Boolean, Bool(true));
        cast!(F64(0.0)                  => Boolean, Bool(false));
        cast!(Null                      => Boolean, Null);

        // Integer
        cast!(Bool(true)            => Int, I64(1));
        cast!(Bool(false)           => Int, I64(0));
        cast!(F64(1.1)              => Int, I64(1));
        cast!(Str("11".to_owned())  => Int, I64(11));
        cast!(Null                  => Int, Null);

        // Float
        cast!(Bool(true)            => Float(None), F64(1.0));
        cast!(Bool(false)           => Float(None), F64(0.0));
        cast!(I64(1)                => Float(None), F64(1.0));
        cast!(Str("11".to_owned())  => Float(None), F64(11.0));
        cast!(Null                  => Float(None), Null);

        // Text
        cast!(Bool(true)    => Text, Str("TRUE".to_owned()));
        cast!(Bool(false)   => Text, Str("FALSE".to_owned()));
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

        cast!(timestamp => Date, date);
        cast!(Null      => Date, Null);

        // Timestamp
        let date = Value::Date(NaiveDate::from_ymd(2021, 5, 1));
        let timestamp = Value::Timestamp(NaiveDate::from_ymd(2021, 5, 1).and_hms(0, 0, 0));

        cast!(date => Timestamp, timestamp);
        cast!(Null => Timestamp, Null);
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
}
