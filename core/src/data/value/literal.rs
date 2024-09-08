use {
    super::{
        date::{parse_date, parse_time, parse_timestamp},
        error::ValueError,
        Value,
    },
    crate::{
        ast::DataType,
        data::{value::uuid::parse_uuid, BigDecimalExt, Interval, Literal, Point},
        result::{Error, Result},
    },
    bigdecimal::BigDecimal,
    chrono::NaiveDate,
    rust_decimal::Decimal,
    std::{
        cmp::Ordering,
        net::{IpAddr, Ipv4Addr, Ipv6Addr},
        str::FromStr,
    },
};

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
    /// Compare this value with a literal.
    pub fn evaluate_eq_with_literal(&self, other: &Literal<'_>) -> Result<Option<bool>> {
        Ok(match (self, other) {
            (Value::Null, _) | (_, Literal::Null) => None,
            (Value::Bool(l), Literal::Boolean(r)) => Some(l == r),
            (Value::I8(l), Literal::Number(r)) => Some(r.to_i8().map(|r| *l == r).unwrap_or(false)),
            (Value::I16(l), Literal::Number(r)) => {
                Some(r.to_i16().map(|r| *l == r).unwrap_or(false))
            }
            (Value::I32(l), Literal::Number(r)) => {
                Some(r.to_i32().map(|r| *l == r).unwrap_or(false))
            }
            (Value::I64(l), Literal::Number(r)) => {
                Some(r.to_i64().map(|r| *l == r).unwrap_or(false))
            }
            (Value::I128(l), Literal::Number(r)) => {
                Some(r.to_i128().map(|r| *l == r).unwrap_or(false))
            }
            (Value::U8(l), Literal::Number(r)) => Some(r.to_u8().map(|r| *l == r).unwrap_or(false)),
            (Value::U16(l), Literal::Number(r)) => {
                Some(r.to_u16().map(|r| *l == r).unwrap_or(false))
            }
            (Value::U32(l), Literal::Number(r)) => {
                Some(r.to_u32().map(|r| *l == r).unwrap_or(false))
            }
            (Value::U64(l), Literal::Number(r)) => {
                Some(r.to_u64().map(|r| *l == r).unwrap_or(false))
            }
            (Value::U128(l), Literal::Number(r)) => {
                Some(r.to_u128().map(|r| *l == r).unwrap_or(false))
            }
            (Value::F32(l), Literal::Number(r)) => {
                Some(r.to_f32().map(|r| *l == r).unwrap_or(false))
            }
            (Value::F64(l), Literal::Number(r)) => {
                Some(r.to_f64().map(|r| *l == r).unwrap_or(false))
            }
            (Value::Str(l), Literal::Text(r)) => Some(l == r.as_ref()),
            (Value::Bytea(l), Literal::Bytea(r)) => Some(l == r),
            (Value::Date(l), Literal::Text(r)) => Some(match r.parse::<NaiveDate>() {
                Ok(r) => l == &r,
                Err(_) => false,
            }),
            (Value::Timestamp(l), Literal::Text(r)) => Some(match parse_timestamp(r) {
                Some(r) => l == &r,
                None => false,
            }),
            (Value::Time(l), Literal::Text(r)) => Some(match parse_time(r) {
                Some(r) => l == &r,
                None => false,
            }),
            (Value::Uuid(l), Literal::Text(r)) => {
                Some(parse_uuid(r).map(|r| l == &r).unwrap_or(false))
            }
            (Value::Inet(l), Literal::Text(r)) => Some(match IpAddr::from_str(r) {
                Ok(x) => l == &x,
                Err(_) => false,
            }),
            (Value::Inet(l), Literal::Number(r)) => Some({
                if let Some(x) = r.to_u32() {
                    l == &Ipv4Addr::from(x)
                } else if let Some(x) = r.to_u128() {
                    l == &Ipv6Addr::from(x)
                } else {
                    false
                }
            }),
            _ => {
                return Err(ValueError::IncompatibleLiteralForDataType {
                    data_type: self.data_type(),
                    literal: format!("{:?}", other),
                }
                .into())
            }
        })
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Value::Bool(_) => DataType::Boolean,
            Value::I8(_) => DataType::Int8,
            Value::I16(_) => DataType::Int16,
            Value::I32(_) => DataType::Int32,
            Value::I64(_) => DataType::Int,
            Value::I128(_) => DataType::Int128,
            Value::U8(_) => DataType::Uint8,
            Value::U16(_) => DataType::Uint16,
            Value::U32(_) => DataType::Uint32,
            Value::U64(_) => DataType::Uint64,
            Value::U128(_) => DataType::Uint128,
            Value::F32(_) => DataType::Float32,
            Value::F64(_) => DataType::Float,
            Value::Decimal(_) => DataType::Decimal,
            Value::Str(_) => DataType::Text,
            Value::Bytea(_) => DataType::Bytea,
            Value::Date(_) => DataType::Date,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Time(_) => DataType::Time,
            Value::Uuid(_) => DataType::Uuid,
            Value::Inet(_) => DataType::Inet,
            Value::Point(_) => DataType::Point,
            Value::Interval(_) => DataType::Interval,
            Value::Map(_) => DataType::Map,
            Value::List(_) => DataType::List,
            Value::Null => DataType::Null,
        }
    }

    pub fn evaluate_cmp_with_literal(&self, other: &Literal<'_>) -> Result<Option<Ordering>> {
        Ok(match (self, other) {
            (Value::Null, _) | (_, Literal::Null) => None,
            (Value::I8(l), Literal::Number(r)) => r.to_i8().map(|r| l.cmp(&r)),
            (Value::I16(l), Literal::Number(r)) => r.to_i16().map(|r| l.cmp(&r)),
            (Value::I32(l), Literal::Number(r)) => r.to_i32().map(|r| l.cmp(&r)),
            (Value::I64(l), Literal::Number(r)) => r.to_i64().map(|r| l.cmp(&r)),
            (Value::I128(l), Literal::Number(r)) => r.to_i128().map(|r| l.cmp(&r)),
            (Value::U8(l), Literal::Number(r)) => r.to_u8().map(|r| l.cmp(&r)),
            (Value::U16(l), Literal::Number(r)) => r.to_u16().map(|r| l.cmp(&r)),
            (Value::U32(l), Literal::Number(r)) => r.to_u32().map(|r| l.cmp(&r)),
            (Value::U64(l), Literal::Number(r)) => r.to_u64().map(|r| l.cmp(&r)),
            (Value::U128(l), Literal::Number(r)) => r.to_u128().map(|r| l.cmp(&r)),
            (Value::F32(l), Literal::Number(r)) => r.to_f32().and_then(|r| l.partial_cmp(&r)),
            (Value::F64(l), Literal::Number(r)) => r.to_f64().and_then(|r| l.partial_cmp(&r)),
            (Value::Decimal(l), Literal::Number(r)) => {
                BigDecimal::new(l.mantissa().into(), l.scale() as i64).partial_cmp(r)
            }
            (Value::Str(l), Literal::Text(r)) => Some(l.as_str().cmp(r)),
            (Value::Date(l), Literal::Text(r)) => r.parse::<NaiveDate>().ok().map(|r| l.cmp(&r)),
            (Value::Timestamp(l), Literal::Text(r)) => parse_timestamp(r).map(|r| l.cmp(&r)),
            (Value::Time(l), Literal::Text(r)) => parse_time(r).map(|r| l.cmp(&r)),
            (Value::Uuid(l), Literal::Text(r)) => parse_uuid(r).ok().map(|r| l.cmp(&r)),
            (Value::Inet(l), Literal::Text(r)) => IpAddr::from_str(r).ok().map(|r| l.cmp(&r)),
            (Value::Inet(l), Literal::Number(r)) => {
                if let Some(x) = r.to_u32() {
                    l.partial_cmp(&Ipv4Addr::from(x))
                } else if let Some(x) = r.to_u128() {
                    l.partial_cmp(&Ipv6Addr::from(x))
                } else {
                    None
                }
            }
            _ => {
                return Err(ValueError::IncompatibleLiteralForDataType {
                    data_type: self.data_type(),
                    literal: format!("{:?}", other),
                }
                .into())
            }
        })
    }

    pub fn try_from_literal(data_type: &DataType, literal: &Literal<'_>) -> Result<Value> {
        match (data_type, literal) {
            (DataType::Boolean, Literal::Boolean(v)) => Ok(Value::Bool(*v)),
            (DataType::Int8, Literal::Number(v)) => v
                .to_i8()
                .map(Value::I8)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Int16, Literal::Number(v)) => v
                .to_i16()
                .map(Value::I16)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Int32, Literal::Number(v)) => v
                .to_i32()
                .map(Value::I32)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Int, Literal::Number(v)) => v
                .to_i64()
                .map(Value::I64)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Int128, Literal::Number(v)) => v
                .to_i128()
                .map(Value::I128)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Uint8, Literal::Number(v)) => v
                .to_u8()
                .map(Value::U8)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Uint16, Literal::Number(v)) => v
                .to_u16()
                .map(Value::U16)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Uint32, Literal::Number(v)) => v
                .to_u32()
                .map(Value::U32)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Uint64, Literal::Number(v)) => v
                .to_u64()
                .map(Value::U64)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Uint128, Literal::Number(v)) => v
                .to_u128()
                .map(Value::U128)
                .ok_or_else(|| ValueError::FailedToParseNumber.into()),
            (DataType::Float32, Literal::Number(v)) => v
                .to_f32()
                .map(Value::F32)
                .ok_or_else(|| ValueError::UnreachableNumberParsing.into()),
            (DataType::Float, Literal::Number(v)) => v
                .to_f64()
                .map(Value::F64)
                .ok_or_else(|| ValueError::UnreachableNumberParsing.into()),
            (DataType::Text, Literal::Text(v)) => Ok(Value::Str(v.to_string())),
            (DataType::Bytea, Literal::Bytea(v)) => Ok(Value::Bytea(v.to_vec())),
            (DataType::Bytea, Literal::Text(v)) => hex::decode(v.as_ref())
                .map(Value::Bytea)
                .map_err(|_| ValueError::FailedToParseHexString(v.to_string()).into()),
            (DataType::Inet, Literal::Text(v)) => IpAddr::from_str(v.as_ref())
                .map(Value::Inet)
                .map_err(|_| ValueError::FailedToParseInetString(v.to_string()).into()),
            (DataType::Inet, Literal::Number(v)) => {
                if let Some(x) = v.to_u32() {
                    Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(x))))
                } else {
                    Ok(Value::Inet(IpAddr::V6(Ipv6Addr::from(
                        v.to_u128().unwrap(),
                    ))))
                }
            }
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
            (DataType::Uuid, Literal::Text(v)) => parse_uuid(v).map(Value::Uuid),
            (DataType::Uuid, Literal::Bytea(v)) => parse_uuid(&hex::encode(v)).map(Value::Uuid),
            (DataType::Map, Literal::Text(v)) => Value::parse_json_map(v),
            (DataType::List, Literal::Text(v)) => Value::parse_json_list(v),
            (DataType::Decimal, Literal::Number(v)) => v
                .to_string()
                .parse::<Decimal>()
                .map(Value::Decimal)
                .map_err(|_| ValueError::FailedToParseDecimal(v.to_string()).into()),
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
                None => Err(ValueError::LiteralCastToInt16Failed(v.to_string()).into()),
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
            (DataType::Uint32, Literal::Text(v)) => v
                .parse::<u32>()
                .map(Value::U32)
                .map_err(|_| ValueError::LiteralCastFromTextToUint32Failed(v.to_string()).into()),
            (DataType::Uint32, Literal::Number(v)) => match v.to_u32() {
                Some(x) => Ok(Value::U32(x)),
                None => Err(ValueError::LiteralCastToUint32Failed(v.to_string()).into()),
            },
            (DataType::Uint32, Literal::Boolean(v)) => {
                let v = u32::from(*v);

                Ok(Value::U32(v))
            }

            (DataType::Uint64, Literal::Text(v)) => v
                .parse::<u64>()
                .map(Value::U64)
                .map_err(|_| ValueError::LiteralCastFromTextToUint64Failed(v.to_string()).into()),
            (DataType::Uint64, Literal::Number(v)) => match v.to_u64() {
                Some(x) => Ok(Value::U64(x)),
                None => Err(ValueError::LiteralCastToUint64Failed(v.to_string()).into()),
            },
            (DataType::Uint64, Literal::Boolean(v)) => {
                let v = u64::from(*v);

                Ok(Value::U64(v))
            }

            (DataType::Uint128, Literal::Text(v)) => v
                .parse::<u128>()
                .map(Value::U128)
                .map_err(|_| ValueError::LiteralCastFromTextToUint128Failed(v.to_string()).into()),
            (DataType::Uint128, Literal::Number(v)) => match v.to_u128() {
                Some(x) => Ok(Value::U128(x)),
                None => Err(ValueError::LiteralCastToUint128Failed(v.to_string()).into()),
            },
            (DataType::Uint128, Literal::Boolean(v)) => {
                let v = u128::from(*v);

                Ok(Value::U128(v))
            }

            (DataType::Float32, Literal::Text(v)) => v
                .parse::<f32>()
                .map(Value::F32)
                .map_err(|_| ValueError::LiteralCastFromTextToFloatFailed(v.to_string()).into()),
            (DataType::Float32, Literal::Number(v)) => {
                v.to_f32().map(Value::F32).ok_or_else(|| {
                    ValueError::UnreachableLiteralCastFromNumberToFloat(v.to_string()).into()
                })
            }
            (DataType::Float32, Literal::Boolean(v)) => {
                let v = if *v { 1.0 } else { 0.0 };

                Ok(Value::F32(v))
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
                Interval::parse(v.as_ref()).map(Value::Interval)
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
            | (DataType::Uint32, Literal::Null)
            | (DataType::Uint64, Literal::Null)
            | (DataType::Uint128, Literal::Null)
            | (DataType::Float32, Literal::Null)
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
            (DataType::Point, Literal::Text(v)) => Point::from_wkt(v)
                .map(Value::Point)
                .map_err(|_| ValueError::FailedToParsePoint(v.to_string()).into()),
            (DataType::Map, Literal::Text(v)) => Value::parse_json_map(v),
            (DataType::List, Literal::Text(v)) => Value::parse_json_list(v),
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
        super::parse_uuid,
        crate::{
            ast::DataType,
            data::{Interval, Literal, Point, Value, ValueError},
            error::ValueError::IncompatibleLiteralForDataType,
        },
        bigdecimal::BigDecimal,
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        rust_decimal::Decimal,
        std::{
            borrow::Cow,
            cmp::Ordering,
            net::{IpAddr, Ipv4Addr, Ipv6Addr},
            str::FromStr,
        },
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
    fn evaluate_eq_with_literal() {
        macro_rules! num {
            ($num: expr) => {
                &Literal::Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }

        macro_rules! text {
            ($text: expr) => {
                &Literal::Text(Cow::Owned($text.to_owned()))
            };
        }

        let uuid_text = "936DA01F9ABD4d9d80C702AF85C822A8";
        let uuid = parse_uuid(uuid_text).unwrap();

        let bytea = || hex::decode("123456").unwrap();
        let inet = |v: &str| Value::Inet(IpAddr::from_str(v).unwrap());

        assert_eq!(
            Ok(Some(true)),
            Value::Bool(true).evaluate_eq_with_literal(&Literal::Boolean(true))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::I8(8).evaluate_eq_with_literal(num!("8"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::I32(32).evaluate_eq_with_literal(num!("32"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::I16(16).evaluate_eq_with_literal(num!("16"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::I32(32).evaluate_eq_with_literal(num!("32"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::I64(64).evaluate_eq_with_literal(num!("64"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::I128(128).evaluate_eq_with_literal(num!("128"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::U8(7).evaluate_eq_with_literal(num!("7"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::U16(64).evaluate_eq_with_literal(num!("64"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::U32(64).evaluate_eq_with_literal(num!("64"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::U64(64).evaluate_eq_with_literal(num!("64"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::U128(64).evaluate_eq_with_literal(num!("64"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::F32(7.123).evaluate_eq_with_literal(num!("7.123"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::F64(7.123).evaluate_eq_with_literal(num!("7.123"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::Str("Hello".to_owned()).evaluate_eq_with_literal(text!("Hello"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::Bytea(bytea()).evaluate_eq_with_literal(&Literal::Bytea(bytea()))
        );
        assert_eq!(
            Ok(Some(true)),
            inet("127.0.0.1").evaluate_eq_with_literal(text!("127.0.0.1"))
        );
        assert_eq!(
            Ok(Some(true)),
            inet("::1").evaluate_eq_with_literal(text!("::1"))
        );
        assert_eq!(
            Ok(Some(true)),
            inet("0.0.0.0").evaluate_eq_with_literal(num!("0"))
        );
        assert_eq!(
            Ok(Some(false)),
            inet("::1").evaluate_eq_with_literal(num!("0"))
        );
        assert_eq!(
            Ok(Some(true)),
            inet("::2:4cb0:16ea").evaluate_eq_with_literal(num!("9876543210"))
        );
        assert_eq!(
            Ok(Some(false)),
            inet("::1").evaluate_eq_with_literal(text!("-1"))
        );
        assert_eq!(
            Ok(Some(false)),
            inet("::1").evaluate_eq_with_literal(num!("-1"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::Date(date(2021, 11, 20)).evaluate_eq_with_literal(text!("2021-11-20"))
        );
        assert_eq!(
            Ok(Some(false)),
            Value::Date(date(2021, 11, 20)).evaluate_eq_with_literal(text!("202=abcdef"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::Timestamp(date_time(2021, 11, 20, 10, 0, 0, 0))
                .evaluate_eq_with_literal(text!("2021-11-20T10:00:00Z"))
        );
        assert_eq!(
            Ok(Some(false)),
            Value::Timestamp(date_time(2021, 11, 20, 10, 0, 0, 0))
                .evaluate_eq_with_literal(text!("2021-11-Hello"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::Time(time(10, 0, 0, 0)).evaluate_eq_with_literal(text!("10:00:00"))
        );
        assert_eq!(
            Ok(Some(false)),
            Value::Time(time(10, 0, 0, 0)).evaluate_eq_with_literal(text!("FALSE"))
        );
        assert_eq!(
            Ok(Some(true)),
            Value::Uuid(uuid).evaluate_eq_with_literal(text!(uuid_text))
        );
        assert_eq!(
            Err(IncompatibleLiteralForDataType {
                data_type: DataType::Int128,
                literal: "Text(\"Hello\")".to_owned()
            }
            .into()),
            Value::I128(67689).evaluate_eq_with_literal(text!("Hello"))
        );
    }

    #[test]
    fn test_value_data_type() {
        assert_eq!(Value::Bool(true).data_type(), DataType::Boolean);
        assert_eq!(Value::I8(1).data_type(), DataType::Int8);
        assert_eq!(Value::I16(1).data_type(), DataType::Int16);
        assert_eq!(Value::I32(1).data_type(), DataType::Int32);
        assert_eq!(Value::I64(1).data_type(), DataType::Int);
        assert_eq!(Value::I128(1).data_type(), DataType::Int128);
        assert_eq!(Value::U8(1).data_type(), DataType::Uint8);
        assert_eq!(Value::U16(1).data_type(), DataType::Uint16);
        assert_eq!(Value::U32(1).data_type(), DataType::Uint32);
        assert_eq!(Value::U64(1).data_type(), DataType::Uint64);
        assert_eq!(Value::U128(1).data_type(), DataType::Uint128);
        assert_eq!(Value::F32(1.0).data_type(), DataType::Float32);
        assert_eq!(Value::F64(1.0).data_type(), DataType::Float);
        assert_eq!(
            Value::Decimal(Decimal::new(1, 0)).data_type(),
            DataType::Decimal
        );
        assert_eq!(Value::Str("Hello".to_owned()).data_type(), DataType::Text);
        assert_eq!(Value::Bytea(vec![1, 2, 3]).data_type(), DataType::Bytea);
        assert_eq!(Value::Date(date(2021, 11, 20)).data_type(), DataType::Date);
        assert_eq!(
            Value::Timestamp(date_time(2021, 11, 20, 10, 0, 0, 0)).data_type(),
            DataType::Timestamp
        );
        assert_eq!(Value::Time(time(10, 0, 0, 0)).data_type(), DataType::Time);
        assert_eq!(
            Value::Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()).data_type(),
            DataType::Uuid
        );
        assert_eq!(
            Value::Inet(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).data_type(),
            DataType::Inet
        );
        assert_eq!(
            Value::Inet(IpAddr::from_str("::2:4cb0:16ea").unwrap()).data_type(),
            DataType::Inet
        );
        assert_eq!(Value::Null.data_type(), DataType::Null);

        assert_eq!(Value::Map(Default::default()).data_type(), DataType::Map);
        assert_eq!(Value::List(Default::default()).data_type(), DataType::List);
        assert_eq!(
            Value::Interval(Interval::Month(1)).data_type(),
            DataType::Interval
        );
        assert_eq!(
            Value::Point(Point::new(0.0, 1.0)).data_type(),
            DataType::Point
        );
    }

    #[test]
    fn evaluate_cmp_with_literal() {
        let num = |n| Literal::Number(Cow::Owned(BigDecimal::from(n)));
        let text = |v: &str| Literal::Text(Cow::Owned(v.to_owned()));

        let test = |value: Value, literal, expected| {
            assert_eq!(value.evaluate_cmp_with_literal(&literal), expected);
        };

        test(Value::I8(1), num(1), Ok(Some(Ordering::Equal)));
        test(Value::I16(1), num(2), Ok(Some(Ordering::Less)));
        test(Value::I32(10), num(3), Ok(Some(Ordering::Greater)));
        test(Value::I64(10), num(10), Ok(Some(Ordering::Equal)));
        test(Value::I128(10), num(10), Ok(Some(Ordering::Equal)));
        test(Value::U8(1), num(1), Ok(Some(Ordering::Equal)));
        test(Value::U16(1), num(2), Ok(Some(Ordering::Less)));
        test(Value::U32(10), num(3), Ok(Some(Ordering::Greater)));
        test(Value::U64(10), num(10), Ok(Some(Ordering::Equal)));
        test(Value::U128(10), num(10), Ok(Some(Ordering::Equal)));
        test(Value::F32(10.0), num(10), Ok(Some(Ordering::Equal)));
        test(Value::F64(10.0), num(10), Ok(Some(Ordering::Equal)));
        test(
            Value::Decimal(Decimal::new(215, 2)),
            num(3),
            Ok(Some(Ordering::Less)),
        );
        test(
            Value::Str("Hello".to_owned()),
            text("Hello"),
            Ok(Some(Ordering::Equal)),
        );
        test(
            Value::Date(date(2021, 11, 21)),
            text("2021-11-21"),
            Ok(Some(Ordering::Equal)),
        );
        test(
            Value::Timestamp(date_time(2021, 11, 21, 10, 0, 0, 0)),
            text("2021-11-21T10:00:00Z"),
            Ok(Some(Ordering::Equal)),
        );
        test(
            Value::Time(time(10, 0, 0, 0)),
            text("10:00:00"),
            Ok(Some(Ordering::Equal)),
        );
        test(
            Value::Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()),
            text("936DA01F9ABD4d9d80C702AF85C822A8"),
            Ok(Some(Ordering::Equal)),
        );
        test(
            Value::Inet(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            text("215.87.1.1"),
            Ok(Some(Ordering::Less)),
        );
        test(
            Value::Inet(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            text("215.87.1.1"),
            Ok(Some(Ordering::Less)),
        );
        test(
            Value::Inet(IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255))),
            Literal::Number(Cow::Owned(BigDecimal::new(4294967295u32.into(), 0))),
            Ok(Some(Ordering::Equal)),
        );
        test(
            Value::Inet(IpAddr::from_str("::2:4cb0:16ea").unwrap()),
            Literal::Number(Cow::Owned(BigDecimal::new(9876543210u128.into(), 0))),
            Ok(Some(Ordering::Equal)),
        );
        test(Value::Null, num(1), Ok(None));

        assert_eq!(
            Value::I8(1).evaluate_cmp_with_literal(&Literal::Boolean(true)),
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Int8,
                literal: "Boolean(true)".to_owned()
            }
            .into())
        );

        assert_eq!(
            Value::I8(1).evaluate_cmp_with_literal(&Literal::Null),
            Ok(None)
        );

        assert_eq!(
            Value::I8(1).evaluate_cmp_with_literal(&Literal::Text(Cow::Owned("1".to_owned()))),
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Int8,
                literal: "Text(\"1\")".to_owned()
            }
            .into())
        );

        // We test the case where we compare an INET with a Number and we fail to convert
        // the provided number to either a u32 or a u128.
        assert_eq!(
            Value::Inet(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).evaluate_cmp_with_literal(
                &Literal::Number(Cow::Owned(BigDecimal::new(1.into(), 100)))
            ),
            Ok(None)
        );
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
        test!(DataType::Uint32, num!("64"), Value::U32(64));
        test!(DataType::Uint64, num!("64"), Value::U64(64));
        test!(DataType::Uint128, num!("64"), Value::U128(64));
        test!(
            DataType::Float32,
            num!("123456789"),
            Value::F32(123456789.0_f32)
        );
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
                assert!(Value::try_from($from)
                    .unwrap()
                    .evaluate_eq(&$expected)
                    .unwrap()
                    .unwrap());
            };
        }

        test!(text!("hello"), Value::Str("hello".to_owned()));
        test!(&text!("hallo"), Value::Str("hallo".to_owned()));
        test!(Literal::Bytea(bytea("1234")), Value::Bytea(bytea("1234")));
        test!(&Literal::Bytea(bytea("1234")), Value::Bytea(bytea("1234")));
        test!(num!("1234567890"), Value::I64(1234567890));
        test!(num!("1.0"), Value::F32(1.0_f32));
        test!(num!("1.0"), Value::F64(1.0));
        test!(&Literal::Boolean(false), Value::Bool(false));
        assert!(matches!(Value::try_from(&Literal::Null), Ok(Value::Null)));

        // Test to check whether comparing incompatible types raises an error.
        assert_eq!(
            Value::try_from(&Literal::Text(Cow::Owned("hello".to_owned())))
                .unwrap()
                .evaluate_eq(&Value::Bool(true)),
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Text,
                literal: "Bool(true)".to_owned()
            }
            .into())
        );
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
                let actual = Value::try_cast_from_literal(&$to, &$from);

                assert_eq!(actual, Ok($expected))
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

        test!(DataType::Uint32, text!("127"), Value::U32(127));
        test!(DataType::Uint32, num!("125"), Value::U32(125));
        test!(DataType::Uint32, Literal::Boolean(true), Value::U32(1));
        test!(DataType::Uint32, Literal::Boolean(false), Value::U32(0));

        test!(DataType::Uint64, text!("127"), Value::U64(127));
        test!(DataType::Uint64, num!("125"), Value::U64(125));
        test!(DataType::Uint64, Literal::Boolean(true), Value::U64(1));
        test!(DataType::Uint64, Literal::Boolean(false), Value::U64(0));

        test!(DataType::Uint128, text!("127"), Value::U128(127));
        test!(DataType::Uint128, num!("125"), Value::U128(125));
        test!(DataType::Uint128, Literal::Boolean(true), Value::U128(1));
        test!(DataType::Uint128, Literal::Boolean(false), Value::U128(0));

        test!(
            DataType::Float32,
            text!("12345.67"),
            Value::F32(12345.67_f32)
        );
        test!(
            DataType::Float32,
            num!("123456.78"),
            Value::F32(123456.78_f32)
        );
        test!(
            DataType::Float32,
            Literal::Boolean(true),
            Value::F32(1.0_f32)
        );
        test!(
            DataType::Float32,
            Literal::Boolean(false),
            Value::F32(0.0_f32)
        );

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
        test_null!(DataType::Uint32, Literal::Null);
        test_null!(DataType::Uint64, Literal::Null);
        test_null!(DataType::Uint128, Literal::Null);
        test_null!(DataType::Float32, Literal::Null);
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
        test!(
            DataType::Map,
            text!(r#"{ "a": 1 }"#),
            Value::parse_json_map(r#"{ "a": 1 }"#).unwrap()
        );
        test!(
            DataType::List,
            text!(r#"[ 1, 2, 3 ]"#),
            Value::parse_json_list(r#"[ 1, 2, 3 ]"#).unwrap()
        );

        // We now cover the corner cases.

        // Literal cast to int8 failed:
        assert_eq!(
            Value::try_cast_from_literal(
                &DataType::Int8,
                &Literal::Number(Cow::Owned(BigDecimal::from_str("128").unwrap()))
            ),
            Err(ValueError::LiteralCastToInt8Failed("128".to_owned()).into())
        );

        // Literal cast to int16 failed:
        assert_eq!(
            Value::try_cast_from_literal(
                &DataType::Int16,
                &Literal::Number(Cow::Owned(BigDecimal::from_str("32768").unwrap()))
            ),
            Err(ValueError::LiteralCastToInt16Failed("32768".to_owned()).into())
        );

        // Literal cast to Uint32 failed:
        assert_eq!(
            Value::try_cast_from_literal(
                &DataType::Uint32,
                &Literal::Number(Cow::Owned(BigDecimal::from_str("4294967296").unwrap()))
            ),
            Err(ValueError::LiteralCastToUint32Failed("4294967296".to_owned()).into())
        );

        // Literal cast to Uint64 failed:
        assert_eq!(
            Value::try_cast_from_literal(
                &DataType::Uint64,
                &Literal::Number(Cow::Owned(
                    BigDecimal::from_str("18446744073709551616").unwrap()
                ))
            ),
            Err(ValueError::LiteralCastToUint64Failed("18446744073709551616".to_owned()).into())
        );

        // Literal cast to Uint128 failed:
        assert_eq!(
            Value::try_cast_from_literal(
                &DataType::Uint128,
                &Literal::Number(Cow::Owned(
                    BigDecimal::from_str("340282366920938463463374607431768211456").unwrap()
                ))
            ),
            Err(ValueError::LiteralCastToUint128Failed(
                "340282366920938463463374607431768211456".to_owned()
            )
            .into())
        );

        // Failing to parse INET string:
        assert_eq!(
            Value::try_cast_from_literal(
                &DataType::Inet,
                &Literal::Text(Cow::Owned("123".to_owned()))
            ),
            Err(ValueError::FailedToParseInetString("123".to_owned()).into())
        );

        assert_eq!(
            Value::try_cast_from_literal(
                &DataType::Inet,
                &Literal::Number(Cow::Owned(
                    BigDecimal::from_str("12354546566547765677657687875748465765764545").unwrap()
                ))
            ),
            Err(ValueError::FailedToParseInetString(
                "12354546566547765677657687875748465765764545".to_owned()
            )
            .into())
        );
    }
}
