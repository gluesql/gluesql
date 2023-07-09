use {
    super::{
        date::{parse_date, parse_time, parse_timestamp},
        Value, ValueError,
    },
    crate::{
        data::{IntervalError, Point},
        result::{Error, Result},
    },
    chrono::{NaiveDate, NaiveDateTime, NaiveTime},
    rust_decimal::prelude::{Decimal, FromPrimitive, FromStr, ToPrimitive},
    std::net::IpAddr,
    uuid::Uuid,
};

impl From<&Value> for String {
    fn from(v: &Value) -> Self {
        match v {
            Value::Str(value) => value.to_owned(),
            Value::Bytea(value) => hex::encode(value),
            Value::Inet(value) => value.to_string(),
            Value::Bool(value) => (if *value { "TRUE" } else { "FALSE" }).to_owned(),
            Value::I8(value) => value.to_string(),
            Value::I16(value) => value.to_string(),
            Value::I32(value) => value.to_string(),
            Value::I64(value) => value.to_string(),
            Value::I128(value) => value.to_string(),
            Value::U8(value) => value.to_string(),
            Value::U16(value) => value.to_string(),
            Value::U32(value) => value.to_string(),
            Value::U64(value) => value.to_string(),
            Value::U128(value) => value.to_string(),
            Value::F32(value) => value.to_string(),
            Value::F64(value) => value.to_string(),
            Value::Date(value) => value.to_string(),
            Value::Timestamp(value) => value.to_string(),
            Value::Time(value) => value.to_string(),
            Value::Interval(value) => value.to_sql_str(),
            Value::Uuid(value) => Uuid::from_u128(*value).to_string(),
            Value::Map(_) => TryInto::<serde_json::Value>::try_into(v.clone())
                .unwrap_or_default()
                .to_string(),
            Value::List(_) => TryInto::<serde_json::Value>::try_into(v.clone())
                .unwrap_or_default()
                .to_string(),
            Value::Decimal(value) => value.to_string(),
            Value::Point(value) => value.to_string(),
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
            Value::U8(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::U16(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::U32(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::U64(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::U128(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::F32(value) => {
                if value.eq(&1.0_f32) {
                    true
                } else if value.eq(&0.0_f32) {
                    false
                } else {
                    return Err(ValueError::ImpossibleCast.into());
                }
            }
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
            | Value::Point(_)
            | Value::Inet(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for i8 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<i8> {
        Ok(match v {
            Value::Bool(value) => i8::from(*value),
            Value::I8(value) => *value,
            Value::I16(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::I32(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::U8(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::U16(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::U32(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => value.to_i8().ok_or(ValueError::ImpossibleCast)?,
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
            | Value::Point(_)
            | Value::Inet(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for i16 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<i16> {
        Ok(match v {
            Value::Bool(value) => i16::from(*value),
            Value::I8(value) => *value as i16,
            Value::I16(value) => *value,
            Value::I32(value) => *value as i16,
            Value::I64(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
            Value::U8(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
            Value::U16(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
            Value::U32(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => value.to_i16().ok_or(ValueError::ImpossibleCast)?,
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
            | Value::Point(_)
            | Value::Inet(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for i32 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<i32> {
        Ok(match v {
            Value::Bool(value) => i32::from(*value),
            Value::I8(value) => *value as i32,
            Value::I16(value) => *value as i32,
            Value::I32(value) => *value,
            Value::I64(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
            Value::U8(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
            Value::U16(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
            Value::U32(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => value.to_i32().ok_or(ValueError::ImpossibleCast)?,
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
            | Value::Point(_)
            | Value::Inet(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for i64 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<i64> {
        Ok(match v {
            Value::Bool(value) => i64::from(*value),
            Value::I8(value) => *value as i64,
            Value::I16(value) => *value as i64,
            Value::I32(value) => *value as i64,
            Value::I64(value) => *value,
            Value::I128(value) => value.to_i64().ok_or(ValueError::ImpossibleCast)?,
            Value::U8(value) => value.to_i64().ok_or(ValueError::ImpossibleCast)?,
            Value::U16(value) => value.to_i64().ok_or(ValueError::ImpossibleCast)?,
            Value::U32(value) => value.to_i64().ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => value.to_i64().ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => value.to_i64().ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => value.to_i64().ok_or(ValueError::ImpossibleCast)?,
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
            | Value::Point(_)
            | Value::Inet(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for i128 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<i128> {
        Ok(match v {
            Value::Bool(value) => i128::from(*value),
            Value::I8(value) => *value as i128,
            Value::I16(value) => *value as i128,
            Value::I32(value) => *value as i128,
            Value::I64(value) => *value as i128,
            Value::I128(value) => *value,
            Value::U8(value) => *value as i128,
            Value::U16(value) => *value as i128,
            Value::U32(value) => value.to_i128().ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => value.to_i128().ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => value.to_i128().ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => value.to_i128().ok_or(ValueError::ImpossibleCast)?,
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
            | Value::Point(_)
            | Value::Inet(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for u8 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<u8> {
        Ok(match v {
            Value::Bool(value) => u8::from(*value),
            Value::I8(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::I16(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::I32(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::U8(value) => *value,
            Value::U16(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::U32(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::F64(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::Str(value) => value
                .parse::<u8>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_u8().ok_or(ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Bytea(_)
            | Value::Point(_)
            | Value::Inet(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}
impl TryFrom<&Value> for u16 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<u16> {
        Ok(match v {
            Value::Bool(value) => u16::from(*value),
            Value::I8(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::I16(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::I32(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::U8(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::U16(value) => *value,
            Value::U32(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::F64(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::Str(value) => value
                .parse::<u16>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_u16().ok_or(ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Bytea(_)
            | Value::Point(_)
            | Value::Inet(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for u32 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<u32> {
        Ok(match v {
            Value::Bool(value) => u32::from(*value),
            Value::I8(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::I16(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::I32(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::U8(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::U16(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::U32(value) => *value,
            Value::U64(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::F64(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::Str(value) => value
                .parse::<u32>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_u32().ok_or(ValueError::ImpossibleCast)?,
            Value::Inet(IpAddr::V4(value)) => u32::from(*value),
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Inet(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Bytea(_)
            | Value::Point(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for u64 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<u64> {
        Ok(match v {
            Value::Bool(value) => u64::from(*value),
            Value::I8(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::I16(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::I32(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::U8(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::U16(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::U32(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => *value,
            Value::U128(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::F64(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::Str(value) => value
                .parse::<u64>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_u64().ok_or(ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Inet(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Bytea(_)
            | Value::Point(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for u128 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<u128> {
        Ok(match v {
            Value::Bool(value) => u128::from(*value),
            Value::I8(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::I16(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::I32(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::U8(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::U16(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::U32(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => *value,
            Value::F32(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::F64(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::Str(value) => value
                .parse::<u128>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_u128().ok_or(ValueError::ImpossibleCast)?,
            Value::Inet(IpAddr::V6(v)) => u128::from(*v),
            Value::Uuid(value) => *value,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Inet(IpAddr::V4(_))
            | Value::Bytea(_)
            | Value::Point(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for f32 {
    type Error = Error;

    fn try_from(v: &Value) -> Result<f32> {
        Ok(match v {
            Value::Bool(value) => {
                if *value {
                    1.0
                } else {
                    0.0
                }
            }
            Value::I8(value) => *value as f32,
            Value::I16(value) => *value as f32,
            Value::I32(value) => *value as f32,
            Value::I64(value) => *value as f32,
            Value::I128(value) => *value as f32,
            Value::U8(value) => *value as f32,
            Value::U16(value) => *value as f32,
            Value::U32(value) => *value as f32,
            Value::U64(value) => *value as f32,
            Value::U128(value) => *value as f32,
            Value::F32(value) => *value,
            Value::F64(value) => *value as f32,
            Value::Str(value) => value
                .parse::<f32>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Decimal(value) => value.to_f32().ok_or(ValueError::ImpossibleCast)?,
            Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Interval(_)
            | Value::Uuid(_)
            | Value::Map(_)
            | Value::List(_)
            | Value::Bytea(_)
            | Value::Point(_)
            | Value::Inet(_)
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
            Value::U8(value) => value.to_f64().ok_or(ValueError::ImpossibleCast)?,
            Value::U16(value) => value.to_f64().ok_or(ValueError::ImpossibleCast)?,
            Value::U32(value) => value.to_f64().ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => value.to_f64().ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => value.to_f64().ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => *value as f64,
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
            | Value::Point(_)
            | Value::Inet(_)
            | Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for usize {
    type Error = Error;

    fn try_from(v: &Value) -> Result<usize> {
        Ok(match v {
            Value::Bool(value) => usize::from(*value),
            Value::I8(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::I16(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::I32(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::I64(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::I128(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::U8(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::U16(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::U32(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => value.to_usize().ok_or(ValueError::ImpossibleCast)?,
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
            | Value::Point(_)
            | Value::Inet(_)
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
            Value::U8(value) => Decimal::from_u8(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::U16(value) => Decimal::from_u16(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::U32(value) => Decimal::from_u32(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::U64(value) => Decimal::from_u64(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::U128(value) => Decimal::from_u128(*value).ok_or(ValueError::ImpossibleCast)?,
            Value::F32(value) => Decimal::from_f32(*value).ok_or(ValueError::ImpossibleCast)?,
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
            | Value::Point(_)
            | Value::Inet(_)
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

try_from_owned_value!(
    bool, i8, i16, i32, i64, i128, f32, f64, u8, u16, u32, u64, u128, usize, Decimal
);

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
            Value::Date(value) => value
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| IntervalError::FailedToParseTime(value.to_string()))?,
            Value::Str(value) => parse_timestamp(value).ok_or(ValueError::ImpossibleCast)?,
            Value::Timestamp(value) => *value,
            _ => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for IpAddr {
    type Error = Error;

    fn try_from(v: &Value) -> Result<IpAddr> {
        Ok(match v {
            Value::Inet(value) => *value,
            Value::Str(value) => IpAddr::from_str(value).map_err(|_| ValueError::ImpossibleCast)?,
            _ => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

impl TryFrom<&Value> for Point {
    type Error = Error;

    fn try_from(v: &Value) -> Result<Point> {
        Ok(match v {
            Value::Point(value) => *value,
            Value::Str(value) => Point::from_wkt(value).map_err(|_| ValueError::ImpossibleCast)?,
            _ => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}

#[cfg(test)]
mod tests {

    use {
        super::{Value, ValueError},
        crate::{data::point, data::Interval as I, data::Point, result::Result},
        chrono::{self, NaiveDate, NaiveDateTime, NaiveTime},
        rust_decimal::Decimal,
        std::{
            collections::HashMap,
            net::{IpAddr, Ipv4Addr, Ipv6Addr},
            str::FromStr,
        },
    };

    fn timestamp(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32, ms: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_milli_opt(hh, mm, ss, ms)
            .unwrap()
    }

    fn time(hour: u32, min: u32, sec: u32, milli: u32) -> NaiveTime {
        NaiveTime::from_hms_milli_opt(hour, min, sec, milli).unwrap()
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    #[test]
    fn from() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!(String::from($from), $to.to_owned())
            };
        }

        test!(Value::Str("text".to_owned()), "text");
        test!(Value::Bytea(hex::decode("1234").unwrap()), "1234");
        test!(Value::Inet(IpAddr::from_str("::1").unwrap()), "::1");
        test!(Value::Bool(true), "TRUE");
        test!(Value::I8(122), "122");
        test!(Value::I16(122), "122");
        test!(Value::I32(122), "122");
        test!(Value::I64(1234567890), "1234567890");
        test!(Value::I128(1234567890), "1234567890");
        test!(Value::U8(122), "122");
        test!(Value::U16(122), "122");
        test!(Value::U32(122), "122");
        test!(Value::U64(122), "122");
        test!(Value::U128(122), "122");
        test!(Value::F32(123456.1_f32), "123456.1");
        test!(Value::F64(1234567890.0987), "1234567890.0987");
        test!(Value::Date(date(2021, 11, 20)), "2021-11-20");
        test!(
            Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
            "2021-11-20 10:00:00"
        );
        test!(Value::Time(time(10, 0, 0, 0)), "10:00:00");
        test!(Value::Interval(I::Month(1)), I::Month(1).to_sql_str());
        test!(
            Value::Uuid(195965723427462096757863453463987888808),
            "936da01f-9abd-4d9d-80c7-02af85c822a8"
        );
        test!(Value::Map(HashMap::new()), "{}");
        test!(Value::List(Vec::new()), "[]");

        let mut map = HashMap::new();
        map.insert("abc".to_owned(), Value::I32(123));
        test!(Value::Map(map), "{\"abc\":123}");
        test!(Value::List(vec![Value::I32(1), Value::I32(2)]), "[1,2]");
        test!(
            Value::Point(point::Point::new(1.0313, 2.0314)),
            "POINT(1.0313 2.0314)"
        );
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
        test!(Value::U8(1), Ok(true));
        test!(Value::U8(0), Ok(false));
        test!(Value::U8(2), Err(ValueError::ImpossibleCast.into()));
        test!(Value::U16(1), Ok(true));
        test!(Value::U16(0), Ok(false));
        test!(Value::U32(1), Ok(true));
        test!(Value::U32(0), Ok(false));
        test!(Value::U64(1), Ok(true));
        test!(Value::U64(0), Ok(false));
        test!(Value::U128(1), Ok(true));
        test!(Value::U128(0), Ok(false));
        test!(Value::U16(2), Err(ValueError::ImpossibleCast.into()));

        test!(Value::F32(1.0_f32), Ok(true));
        test!(Value::F32(0.0_f32), Ok(false));
        test!(Value::F32(2.0_f32), Err(ValueError::ImpossibleCast.into()));
        test!(Value::F64(1.0), Ok(true));
        test!(Value::F64(0.0), Ok(false));
        test!(Value::F64(2.0), Err(ValueError::ImpossibleCast.into()));
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
        test!(
            Value::Point(point::Point::new(1.0313, 2.0314)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));

        // impossible casts
        test!(Value::I8(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I16(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I32(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I64(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I128(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::U8(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::U16(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::U32(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::U64(3), Err(ValueError::ImpossibleCast.into()));
        test!(Value::U128(3), Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_i8() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i8>, $to);
                assert_eq!(i8::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::U8(122), Ok(122));
        test!(Value::U16(122), Ok(122));
        test!(Value::U32(122), Ok(122));
        test!(Value::U64(122), Ok(122));
        test!(Value::U128(122), Ok(122));
        test!(Value::F32(122.0_f32), Ok(122));
        test!(Value::F64(122.0), Ok(122));
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
        test!(Value::U8(128), Err(ValueError::ImpossibleCast.into()));
        test!(Value::U16(128), Err(ValueError::ImpossibleCast.into()));
        test!(Value::U32(128), Err(ValueError::ImpossibleCast.into()));
        test!(Value::U64(128), Err(ValueError::ImpossibleCast.into()));
        test!(Value::U128(128), Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::F32(128.0_f32),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::F64(128.0), Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::Point(point::Point::new(1.0313, 2.0314)),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_i16() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i16>, $to);
                assert_eq!(i16::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::U8(122), Ok(122));
        test!(Value::U16(122), Ok(122));
        test!(Value::U32(122), Ok(122));
        test!(Value::U64(122), Ok(122));
        test!(Value::U128(122), Ok(122));
        test!(Value::F32(122.0_f32), Ok(122));
        test!(Value::F32(122.1_f32), Ok(122));
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
        test!(
            Value::Point(point::Point::new(1.0313, 2.0314)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_i32() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i32>, $to);
                assert_eq!(i32::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::U8(122), Ok(122));
        test!(Value::U16(122), Ok(122));
        test!(Value::U32(122), Ok(122));
        test!(Value::U64(122), Ok(122));
        test!(Value::U128(122), Ok(122));
        test!(Value::I64(1234567890), Ok(1234567890));
        test!(Value::F32(1234567890.0_f32), Ok(1234567890.0_f32 as i32));
        test!(Value::F32(1234567890.1_f32), Ok(1234567890.1_f32 as i32));
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
        test!(
            Value::Point(point::Point::new(1.0313, 2.0314)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_i64() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i64>, $to);
                assert_eq!(i64::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::U8(122), Ok(122));
        test!(Value::U16(122), Ok(122));
        test!(Value::U32(122), Ok(122));
        test!(Value::U64(122), Ok(122));
        test!(Value::U128(122), Ok(122));
        test!(Value::I64(1234567890), Ok(1234567890));
        test!(Value::F32(1234567890.0_f32), Ok(1234567890.0_f32 as i64));
        test!(Value::F32(1234567890.1_f32), Ok(1234567890.1_f32 as i64));
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
        test!(
            Value::Point(point::Point::new(1.0313, 2.0314)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_i128() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<i128>, $to);
                assert_eq!(i128::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::U8(122), Ok(122));
        test!(Value::U16(122), Ok(122));
        test!(Value::U32(122), Ok(122));
        test!(Value::U64(122), Ok(122));
        test!(Value::U128(122), Ok(122));
        test!(Value::I64(1234567890), Ok(1234567890));
        test!(Value::F32(1234567890.0_f32), Ok(1234567890.0_f32 as i128));
        test!(Value::F32(1234567890.9_f32), Ok(1234567890.9_f32 as i128));
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
        test!(
            Value::Point(point::Point::new(1.0313, 2.0314)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_u8() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<u8>, $to);
                assert_eq!(u8::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::U8(122), Ok(122));
        test!(Value::U16(122), Ok(122));
        test!(Value::U32(122), Ok(122));
        test!(Value::U64(122), Ok(122));
        test!(Value::U128(122), Ok(122));
        test!(Value::F32(122.0_f32), Ok(122));
        test!(Value::F32(122.9_f32), Ok(122));
        test!(Value::F64(122.0), Ok(122));
        test!(Value::F64(122.9), Ok(122));
        test!(Value::Str("122".to_owned()), Ok(122));
        test!(Value::Decimal(Decimal::new(123, 0)), Ok(123));

        test!(
            Value::List(Vec::new()),
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
        test!(
            Value::Point(point::Point::new(1.0313, 2.0314)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));

        // impossible casts to u8
        test!(Value::I16(256), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I32(256), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I64(256), Err(ValueError::ImpossibleCast.into()));
        test!(Value::I128(256), Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::F32(256.0_f32),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::F64(256.0), Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_u16() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<u16>, $to);
                assert_eq!(u16::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::U8(122), Ok(122));
        test!(Value::U16(122), Ok(122));
        test!(Value::U32(122), Ok(122));
        test!(Value::U64(122), Ok(122));
        test!(Value::U128(122), Ok(122));
        test!(Value::F32(122.0_f32), Ok(122));
        test!(Value::F32(122.1_f32), Ok(122));
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
    fn try_into_u32() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<u32>, $to);
                assert_eq!(u32::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::U8(122), Ok(122));
        test!(Value::U16(122), Ok(122));
        test!(Value::U32(122), Ok(122));
        test!(Value::U64(122), Ok(122));
        test!(Value::U128(122), Ok(122));
        test!(Value::F32(122.0_f32), Ok(122));
        test!(Value::F32(122.1_f32), Ok(122));
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
        assert_eq!(
            u32::try_from(&Value::Inet(IpAddr::from_str("0.0.0.0").unwrap())),
            Ok(u32::from(Ipv4Addr::from(0)))
        );
        assert_eq!(
            u32::try_from(&Value::Inet(IpAddr::from_str("::0").unwrap())),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_u64() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<u64>, $to);
                assert_eq!(u64::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::U8(122), Ok(122));
        test!(Value::U16(122), Ok(122));
        test!(Value::U32(122), Ok(122));
        test!(Value::U64(122), Ok(122));
        test!(Value::U128(122), Ok(122));
        test!(Value::F32(122.0_f32), Ok(122));
        test!(Value::F32(122.1_f32), Ok(122));
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
        test!(
            Value::Point(point::Point::new(1.0313, 2.0314)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_u128() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<u128>, $to);
                assert_eq!(u128::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::U8(122), Ok(122));
        test!(Value::U16(122), Ok(122));
        test!(Value::U32(122), Ok(122));
        test!(Value::U64(122), Ok(122));
        test!(Value::U128(122), Ok(122));
        test!(Value::F32(122.0_f32), Ok(122));
        test!(Value::F32(122.1_f32), Ok(122));
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
            Value::Map(HashMap::new()),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(
            Value::List(Vec::new()),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
        let uuid = 195965723427462096757863453463987888808;
        assert_eq!((&Value::Uuid(uuid)).try_into() as Result<u128>, Ok(uuid));
        assert_eq!(u128::try_from(&Value::Uuid(uuid)), Ok(uuid));

        let ip = Ipv6Addr::from(9876543210);
        assert_eq!(
            u128::try_from(&Value::Inet(IpAddr::V6(ip))),
            Ok(u128::from(ip))
        );

        assert_eq!(
            u128::try_from(&Value::Date(date(2021, 11, 20))),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_f32() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<f32>, $to);
                assert_eq!(f32::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1.0_f32));
        test!(Value::Bool(false), Ok(0.0_f32));
        test!(Value::I8(122), Ok(122.0_f32));
        test!(Value::I16(122), Ok(122.0_f32));
        test!(Value::I32(122), Ok(122.0_f32));
        test!(Value::I64(122), Ok(122.0_f32));
        test!(Value::I128(122), Ok(122.0_f32));
        test!(Value::U8(122), Ok(122.0_f32));
        test!(Value::U16(122), Ok(122.0_f32));
        test!(Value::U32(122), Ok(122.0_f32));
        test!(Value::U64(122), Ok(122.0_f32));
        test!(Value::U128(122), Ok(122.0_f32));
        test!(Value::I64(1234567890), Ok(1234567890.0_f32));
        test!(Value::F64(1234567890.1), Ok(1234567890.1_f32));
        test!(Value::F32(1234567890.1_f32), Ok(1234567890.1_f32));
        test!(Value::Str("1234567890.1".to_owned()), Ok(1234567890.1_f32));
        test!(
            Value::Decimal(Decimal::new(12345678901, 1)),
            Ok(1234567890.1_f32)
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
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_f64() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<f64>, $to);
                assert_eq!(f64::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1.0));
        test!(Value::Bool(false), Ok(0.0));
        test!(Value::I8(122), Ok(122.0));
        test!(Value::I16(122), Ok(122.0));
        test!(Value::I32(122), Ok(122.0));
        test!(Value::I64(122), Ok(122.0));
        test!(Value::I128(122), Ok(122.0));
        test!(Value::U8(122), Ok(122.0));
        test!(Value::U16(122), Ok(122.0));
        test!(Value::U32(122), Ok(122.0));
        test!(Value::U64(122), Ok(122.0));
        test!(Value::U128(122), Ok(122.0));
        test!(Value::I64(1234567890), Ok(1234567890.0));
        test!(Value::F32(1234567890.1_f32), Ok(1234567890.1_f32 as f64));
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
        test!(
            Value::Point(point::Point::new(1.0313, 2.0314)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_usize() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<usize>, $to);
                assert_eq!(usize::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(1usize));
        test!(Value::Bool(false), Ok(0));
        test!(Value::I8(122), Ok(122));
        test!(Value::I16(122), Ok(122));
        test!(Value::I32(122), Ok(122));
        test!(Value::I64(122), Ok(122));
        test!(Value::I128(122), Ok(122));
        test!(Value::U8(122), Ok(122));
        test!(Value::U16(122), Ok(122));
        test!(Value::U32(122), Ok(122));
        test!(Value::U64(122), Ok(122));
        test!(Value::U128(122), Ok(122));
        test!(Value::I64(1234567890), Ok(1234567890));
        test!(Value::F32(1234567890.0_f32), Ok(1234567890.0_f32 as usize));
        test!(Value::F32(1234567890.1_f32), Ok(1234567890.1_f32 as usize));
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
        test!(
            Value::Point(point::Point::new(1.0313, 2.0314)),
            Err(ValueError::ImpossibleCast.into())
        );
        test!(Value::Null, Err(ValueError::ImpossibleCast.into()));
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_decimal() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<Decimal>, $to);
                assert_eq!(Decimal::try_from($from), $to);
            };
        }

        test!(Value::Bool(true), Ok(Decimal::new(1, 0)));
        test!(Value::Bool(false), Ok(Decimal::new(0, 0)));
        test!(Value::I8(122), Ok(Decimal::new(122, 0)));
        test!(Value::I16(122), Ok(Decimal::new(122, 0)));
        test!(Value::I32(122), Ok(Decimal::new(122, 0)));
        test!(Value::I64(122), Ok(Decimal::new(122, 0)));
        test!(Value::I128(122), Ok(Decimal::new(122, 0)));
        test!(Value::U8(122), Ok(Decimal::new(122, 0)));
        test!(Value::U16(122), Ok(Decimal::new(122, 0)));
        test!(Value::U32(122), Ok(Decimal::new(122, 0)));
        test!(Value::U64(122), Ok(Decimal::new(122, 0)));
        test!(Value::U128(122), Ok(Decimal::new(122, 0)));
        test!(Value::F32(122.0_f32), Ok(Decimal::new(122, 0)));
        test!(Value::F32(122.1_f32), Ok(Decimal::new(1221, 1)));
        test!(Value::F64(122.0), Ok(Decimal::new(122, 0)));
        test!(Value::F64(122.1), Ok(Decimal::new(1221, 1)));
        test!(Value::Str("122".to_owned()), Ok(Decimal::new(122, 0)));
        test!(
            Value::Decimal(Decimal::new(122, 0)),
            Ok(Decimal::new(122, 0))
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
        test!(
            Value::Inet(IpAddr::from_str("::1").unwrap()),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_naive_date() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<chrono::NaiveDate>, $to);
                assert_eq!(chrono::NaiveDate::try_from($from), $to);
            };
        }

        test!(&Value::Date(date(2021, 11, 20)), Ok(date(2021, 11, 20)));
        test!(
            &Value::Timestamp(timestamp(2021, 11, 20, 10, 0, 0, 0)),
            Ok(date(2021, 11, 20))
        );
        test!(&Value::Str("2021-11-20".to_owned()), Ok(date(2021, 11, 20)));
        test!(&Value::F32(1.0_f32), Err(ValueError::ImpossibleCast.into()));
        test!(&Value::F64(1.0), Err(ValueError::ImpossibleCast.into()));
    }

    #[test]
    fn try_into_naive_time() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<chrono::NaiveTime>, $to);
                assert_eq!(chrono::NaiveTime::try_from($from), $to);
            };
        }

        test!(&Value::Time(time(10, 0, 0, 0)), Ok(time(10, 0, 0, 0)));
        test!(&Value::Str("10:00:00".to_owned()), Ok(time(10, 0, 0, 0)));
        test!(&Value::F32(1.0_f32), Err(ValueError::ImpossibleCast.into()));
        test!(&Value::F64(1.0), Err(ValueError::ImpossibleCast.into()));
    }

    #[test]
    fn try_into_naive_date_time() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!($from.try_into() as Result<chrono::NaiveDateTime>, $to);
                assert_eq!(chrono::NaiveDateTime::try_from($from), $to);
            };
        }

        let datetime = chrono::NaiveDateTime::new;
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
        test!(&Value::F32(1.0_f32), Err(ValueError::ImpossibleCast.into()));
        test!(&Value::F64(1.0), Err(ValueError::ImpossibleCast.into()));
    }

    #[test]
    fn try_into_ipaddr() {
        macro_rules! test {
            ($from: expr, $to: expr) => {
                assert_eq!(IpAddr::try_from($from), Ok(IpAddr::from_str($to).unwrap()));
                assert_eq!(IpAddr::try_from($from), Ok(IpAddr::from_str($to).unwrap()))
            };
        }
        test!(&Value::Inet(IpAddr::from_str("::1").unwrap()), "::1");
        test!(&Value::Str("127.0.0.1".to_owned()), "127.0.0.1");
        test!(&Value::Str("0.0.0.0".to_owned()), "0.0.0.0");
        test!(IpAddr::from_str("::1").unwrap(), "::1");
        test!(IpAddr::from_str("::2:4cb0:16ea").unwrap(), "::2:4cb0:16ea");
        assert_eq!(
            IpAddr::try_from(&Value::Date(date(2021, 11, 20))),
            Err(ValueError::ImpossibleCast.into())
        );
    }

    #[test]
    fn try_into_point() {
        assert_eq!(
            Point::try_from(&Value::Str("POINT(0.2 0.1)".to_owned())),
            Ok(Point::from_wkt("POINT(0.2 0.1)").unwrap())
        );
        assert_eq!(
            Point::try_from(&Value::Point(Point::new(0.1, 0.2))),
            Ok(Point::from_wkt("POINT(0.1 0.2)").unwrap())
        );
        assert_eq!(
            Point::try_from(&Value::Date(date(2021, 11, 20))),
            Err(ValueError::ImpossibleCast.into())
        );
    }
}
