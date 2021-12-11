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
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
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
