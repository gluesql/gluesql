use {
    super::{Interval, Value},
    crate::{
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
    Null,
}

fn try_i128_to_json(v: i128) -> Result<JsonValue> {
    JsonNumber::from_str(&v.to_string())
        .map(JsonValue::Number)
        .map_err(|_| ValueError::UnreachableJsonNumberParseFailure(v.to_string()).into())
}

fn try_i128_vec_to_json(v: Vec<i128>) -> Result<JsonValue> {
    v.into_iter().map(try_i128_to_json).collect()
}

fn try_u128_to_json(v: u128) -> Result<JsonValue> {
    JsonNumber::from_str(&v.to_string())
        .map(JsonValue::Number)
        .map_err(|_| ValueError::UnreachableJsonNumberParseFailure(v.to_string()).into())
}

fn try_u128_vec_to_json(v: Vec<u128>) -> Result<JsonValue> {
    v.into_iter().map(try_u128_to_json).collect()
}

fn try_decimal_to_json(v: Decimal) -> Result<JsonValue> {
    JsonNumber::from_str(&v.to_string())
        .map(JsonValue::Number)
        .map_err(|_| ValueError::UnreachableJsonNumberParseFailure(v.to_string()).into())
}

fn try_decimal_vec_to_json(v: Vec<Decimal>) -> Result<JsonValue> {
    v.into_iter().map(try_decimal_to_json).collect()
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

impl TryFrom<ArrayValue> for JsonValue {
    type Error = Error;

    fn try_from(value: ArrayValue) -> Result<Self> {
        match value {
            ArrayValue::Bool(v) => Ok(JsonValue::from(v)),
            ArrayValue::I8(v) => Ok(JsonValue::from(v)),
            ArrayValue::I16(v) => Ok(JsonValue::from(v)),
            ArrayValue::I32(v) => Ok(JsonValue::from(v)),
            ArrayValue::I64(v) => Ok(JsonValue::from(v)),
            ArrayValue::I128(v) => try_i128_vec_to_json(v),
            ArrayValue::U8(v) => Ok(JsonValue::from(v)),
            ArrayValue::U16(v) => Ok(JsonValue::from(v)),
            ArrayValue::U32(v) => Ok(JsonValue::from(v)),
            ArrayValue::U64(v) => Ok(JsonValue::from(v)),
            ArrayValue::U128(v) => try_u128_vec_to_json(v),
            ArrayValue::F32(v) => Ok(JsonValue::from(v)),
            ArrayValue::F64(v) => Ok(JsonValue::from(v)),
            ArrayValue::Decimal(v) => try_decimal_vec_to_json(v),
            ArrayValue::Str(v) => Ok(JsonValue::from(v)),
            ArrayValue::Bytea(v) => Ok(JsonValue::from(v)),
            ArrayValue::Inet(v) => Ok(JsonValue::from(
                v.into_iter()
                    .map(|item| item.to_string())
                    .collect::<Vec<String>>(),
            )),
            ArrayValue::Date(v) => Ok(JsonValue::from(
                v.into_iter()
                    .map(|item| item.to_string())
                    .collect::<Vec<String>>(),
            )),
            ArrayValue::Timestamp(v) => Ok(JsonValue::from(
                v.into_iter()
                    .map(|item| Utc.from_utc_datetime(&item).to_string())
                    .collect::<Vec<String>>(),
            )),
            ArrayValue::Time(v) => Ok(JsonValue::from(
                v.into_iter()
                    .map(|item| item.to_string())
                    .collect::<Vec<String>>(),
            )),
            ArrayValue::Interval(v) => Ok(JsonValue::from(
                v.into_iter()
                    .map(|item| item.to_sql_str())
                    .collect::<Vec<String>>(),
            )),
            ArrayValue::Uuid(v) => Ok(JsonValue::from(
                v.into_iter()
                    .map(|item| Uuid::from_u128(item).hyphenated().to_string())
                    .collect::<Vec<String>>(),
            )),
            ArrayValue::Map(v) => try_map_vec_to_json(v),
            ArrayValue::List(v) => try_list_vec_to_json(v),
            ArrayValue::Array(v) => v.into_iter().map(JsonValue::try_from).collect(),
            ArrayValue::Point(v) => Ok(JsonValue::from(
                v.into_iter()
                    .map(|item| item.to_string())
                    .collect::<Vec<String>>(),
            )),
            ArrayValue::Null => Ok(JsonValue::Null),
        }
    }
}
