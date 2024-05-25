use {
    super::{Interval, Value},
    crate::{
        ast::DataType,
        data::point::Point,
        result::{Error, Result, ValueError},
    },
    chrono::{offset::Utc, NaiveDate, NaiveDateTime, NaiveTime, TimeZone},
    rust_decimal::Decimal,
    serde::{Deserialize, Serialize},
    serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue},
    std::{collections::HashMap, net::IpAddr, str::FromStr},
    uuid::Uuid,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ArrayValue {
    Bool(Vec<bool>),
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    I128(Vec<i128>),
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    U128(Vec<u128>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Decimal(Vec<Decimal>),
    Str(Vec<String>),
    Bytea(Vec<Vec<u8>>),
    Inet(Vec<IpAddr>),
    Date(Vec<NaiveDate>),
    Timestamp(Vec<NaiveDateTime>),
    Time(Vec<NaiveTime>),
    Interval(Vec<Interval>),
    Uuid(Vec<u128>),
    Map(Vec<HashMap<String, Value>>),
    List(Vec<Vec<Value>>),
    Array(Vec<ArrayValue>),
    Point(Vec<Point>),
}

impl ArrayValue {
    pub fn get_type(&self) -> DataType {
        match self {
            ArrayValue::Bool(_) => DataType::Boolean,
            ArrayValue::I8(_) => DataType::Int8,
            ArrayValue::I16(_) => DataType::Int16,
            ArrayValue::I32(_) => DataType::Int32,
            ArrayValue::I64(_) => DataType::Int,
            ArrayValue::I128(_) => DataType::Int128,
            ArrayValue::U8(_) => DataType::Uint8,
            ArrayValue::U16(_) => DataType::Uint16,
            ArrayValue::U32(_) => DataType::Uint32,
            ArrayValue::U64(_) => DataType::Uint64,
            ArrayValue::U128(_) => DataType::Uint128,
            ArrayValue::F32(_) => DataType::Float32,
            ArrayValue::F64(_) => DataType::Float,
            ArrayValue::Decimal(_) => DataType::Decimal,
            ArrayValue::Str(_) => DataType::Text,
            ArrayValue::Bytea(_) => DataType::Bytea,
            ArrayValue::Inet(_) => DataType::Inet,
            ArrayValue::Date(_) => DataType::Date,
            ArrayValue::Timestamp(_) => DataType::Timestamp,
            ArrayValue::Time(_) => DataType::Time,
            ArrayValue::Interval(_) => DataType::Interval,
            ArrayValue::Uuid(_) => DataType::Uuid,
            ArrayValue::Map(_) => DataType::Map,
            ArrayValue::List(_) => DataType::List,
            ArrayValue::Array(array_values) => {
                let data_type = array_values
                    .first()
                    .map_or(DataType::Text, |value| value.get_type());
                let array_length = array_values.len();

                DataType::Array(Box::new(data_type), Some(array_length))
            }
            ArrayValue::Point(_) => DataType::Point,
        }
    }
}

fn try_big_number_to_json<T>(big_number: T) -> Result<JsonValue>
where
    T: ToString,
{
    JsonNumber::from_str(&big_number.to_string())
        .map(JsonValue::Number)
        .map_err(|_| ValueError::UnreachableJsonNumberParseFailure(big_number.to_string()).into())
}

fn try_big_number_vec_to_json<T>(big_number_vec: Vec<T>) -> Result<JsonValue>
where
    T: ToString,
{
    big_number_vec
        .into_iter()
        .map(try_big_number_to_json)
        .collect()
}

fn try_map_to_json(v: HashMap<String, Value>) -> Result<JsonValue> {
    v.into_iter()
        .map(|(key, value)| value.try_into().map(|value| (key, value)))
        .collect::<Result<Vec<(String, JsonValue)>>>()
        .map(|v| JsonMap::from_iter(v).into())
}

fn try_map_vec_to_json(v: Vec<HashMap<String, Value>>) -> Result<JsonValue> {
    v.into_iter().map(try_map_to_json).collect()
}

fn try_list_to_json(v: Vec<Value>) -> Result<JsonValue> {
    v.into_iter()
        .map(|value| value.try_into())
        .collect::<Result<Vec<JsonValue>>>()
        .map(|v| v.into())
}

fn try_list_vec_to_json(v: Vec<Vec<Value>>) -> Result<JsonValue> {
    v.into_iter().map(try_list_to_json).collect()
}

fn try_to_string_vec_to_json<T>(v: Vec<T>) -> Result<JsonValue>
where
    T: ToString,
{
    Ok(JsonValue::from(
        v.into_iter()
            .map(|item| item.to_string())
            .collect::<Vec<String>>(),
    ))
}

fn try_stringy_vec_to_json<T, U>(v: Vec<T>, stringifier: fn(T) -> U) -> Result<JsonValue>
where
    U: ToString,
{
    Ok(JsonValue::from(
        v.into_iter()
            .map(|item| stringifier(item).to_string())
            .collect::<Vec<String>>(),
    ))
}

impl TryFrom<ArrayValue> for JsonValue {
    type Error = Error;

    fn try_from(value: ArrayValue) -> Result<Self> {
        match value {
            ArrayValue::Bool(v) => Ok(JsonValue::from(v)),
            ArrayValue::I8(v) => Ok(JsonValue::from(v)),
            ArrayValue::I16(v) => Ok(JsonValue::from(v)),
            ArrayValue::I32(v) => Ok(JsonValue::from(v)),
            ArrayValue::I64(v) => Ok(JsonValue::from(v)),
            ArrayValue::I128(v) => try_big_number_vec_to_json(v),
            ArrayValue::U8(v) => Ok(JsonValue::from(v)),
            ArrayValue::U16(v) => Ok(JsonValue::from(v)),
            ArrayValue::U32(v) => Ok(JsonValue::from(v)),
            ArrayValue::U64(v) => Ok(JsonValue::from(v)),
            ArrayValue::U128(v) => try_big_number_vec_to_json(v),
            ArrayValue::F32(v) => Ok(JsonValue::from(v)),
            ArrayValue::F64(v) => Ok(JsonValue::from(v)),
            ArrayValue::Decimal(v) => try_big_number_vec_to_json(v),
            ArrayValue::Str(v) => Ok(JsonValue::from(v)),
            ArrayValue::Bytea(v) => Ok(JsonValue::from(v)),
            ArrayValue::Inet(v) => try_to_string_vec_to_json(v),
            ArrayValue::Date(v) => try_to_string_vec_to_json(v),
            ArrayValue::Timestamp(v) => {
                try_stringy_vec_to_json(v, |item| Utc.from_utc_datetime(&item))
            }
            ArrayValue::Time(v) => try_to_string_vec_to_json(v),
            // It is unwise to use `try_stringy_vec_to_json` for `Interval`
            // because `item.to_sql_str()` already returns `String`,
            // hence calling `to_string()` again in `try_stringy_vec_to_json` will
            // clone the `String` which is unnecessary and not performant.
            ArrayValue::Interval(v) => Ok(JsonValue::from(
                v.into_iter()
                    .map(|item| item.to_sql_str())
                    .collect::<Vec<String>>(),
            )),
            ArrayValue::Uuid(v) => {
                try_stringy_vec_to_json(v, |item| Uuid::from_u128(item).hyphenated())
            }
            ArrayValue::Map(v) => try_map_vec_to_json(v),
            ArrayValue::List(v) => try_list_vec_to_json(v),
            ArrayValue::Array(v) => v.into_iter().map(JsonValue::try_from).collect(),
            ArrayValue::Point(v) => try_to_string_vec_to_json(v),
        }
    }
}
