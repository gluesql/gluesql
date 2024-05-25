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
    /// Returns the data type of the array.
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

    /// Returns the length of the array.
    /// Returns `None` if the array is empty.
    pub fn get_length(&self) -> Option<usize> {
        match self {
            ArrayValue::Bool(v) => Some(v.len()),
            ArrayValue::I8(v) => Some(v.len()),
            ArrayValue::I16(v) => Some(v.len()),
            ArrayValue::I32(v) => Some(v.len()),
            ArrayValue::I64(v) => Some(v.len()),
            ArrayValue::I128(v) => Some(v.len()),
            ArrayValue::U8(v) => Some(v.len()),
            ArrayValue::U16(v) => Some(v.len()),
            ArrayValue::U32(v) => Some(v.len()),
            ArrayValue::U64(v) => Some(v.len()),
            ArrayValue::U128(v) => Some(v.len()),
            ArrayValue::F32(v) => Some(v.len()),
            ArrayValue::F64(v) => Some(v.len()),
            ArrayValue::Decimal(v) => Some(v.len()),
            ArrayValue::Str(v) => Some(v.len()),
            ArrayValue::Bytea(v) => Some(v.len()),
            ArrayValue::Inet(v) => Some(v.len()),
            ArrayValue::Date(v) => Some(v.len()),
            ArrayValue::Timestamp(v) => Some(v.len()),
            ArrayValue::Time(v) => Some(v.len()),
            ArrayValue::Interval(v) => Some(v.len()),
            ArrayValue::Uuid(v) => Some(v.len()),
            ArrayValue::Map(v) => Some(v.len()),
            ArrayValue::List(v) => Some(v.len()),
            ArrayValue::Array(v) => Some(v.len()),
            ArrayValue::Point(v) => Some(v.len()),
        }
    }

    pub fn from_values(data_type: &DataType, values: Vec<Value>) -> Result<ArrayValue> {
        /// Macro to convert data type to array value.
        macro_rules! dt_to_av {
            ( $array_value_type:ident, $value:ident) => {
                values
                    .into_iter()
                    .map(|value| {
                        if let Value::$value(v) = value {
                            Ok(v)
                        } else {
                            Err(ValueError::IncompatibleDataType {
                                data_type: data_type.to_owned(),
                                value,
                            }
                            .into())
                        }
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(ArrayValue::$array_value_type)
            };
        }

        match *data_type {
            DataType::Boolean => dt_to_av!(Bool, Bool),
            DataType::Int8 => dt_to_av!(I64, I64),
            DataType::Int16 => dt_to_av!(I16, I16),
            DataType::Int32 => dt_to_av!(I32, I32),
            DataType::Int => dt_to_av!(I64, I64),
            DataType::Int128 => dt_to_av!(I128, I128),
            DataType::Uint8 => dt_to_av!(U8, U8),
            DataType::Uint16 => dt_to_av!(U16, U16),
            DataType::Uint32 => dt_to_av!(U32, U32),
            DataType::Uint64 => dt_to_av!(U64, U64),
            DataType::Uint128 => dt_to_av!(U128, U128),
            DataType::Float32 => dt_to_av!(F32, F32),
            DataType::Float => dt_to_av!(F64, F64),
            DataType::Text => dt_to_av!(Str, Str),
            DataType::Bytea => dt_to_av!(Bytea, Bytea),
            DataType::Inet => dt_to_av!(Inet, Inet),
            DataType::Date => dt_to_av!(Date, Date),
            DataType::Timestamp => dt_to_av!(Timestamp, Timestamp),
            DataType::Time => dt_to_av!(Time, Time),
            DataType::Interval => dt_to_av!(Interval, Interval),
            DataType::Uuid => dt_to_av!(Uuid, Uuid),
            DataType::Map => dt_to_av!(Map, Map),
            DataType::List => dt_to_av!(List, List),
            DataType::Decimal => dt_to_av!(Decimal, Decimal),
            DataType::Point => dt_to_av!(Point, Point),
            DataType::Array(_, _) => dt_to_av!(Array, Array),
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

impl Value {
    pub fn parse_typed_array(
        data_type: &DataType,
        array_length: Option<usize>,
        value: &str,
    ) -> Result<Value> {
        // Check if value is surrounded by curly brackets
        if !value.starts_with('{') || !value.ends_with('}') {
            return Err(ValueError::InvalidArrayBrackets(value.to_owned()).into());
        }

        let value = value.replace("{", "[").replace("}", "]");
        let value = serde_json::from_str(&value)
            .map_err(|_| ValueError::InvalidArrayString(value.to_owned()))?;

        match value {
            JsonValue::Array(json_array) => {
                if let Some(array_length) = array_length {
                    if json_array.len() > array_length {
                        return Err(ValueError::ArrayOverflow {
                            expected: array_length,
                            received: json_array.len(),
                        }
                        .into());
                    }
                }

                json_array
                    .into_iter()
                    .map(|value| match value {
                        serde_json::Value::Null => {
                            Err(ValueError::NullValueInArray(value.to_string()).into())
                        }
                        _ => value.try_into(),
                    })
                    .collect::<Result<Vec<Value>>>()
                    .and_then(|values| ArrayValue::from_values(data_type, values))
                    .map(Value::Array)
            }
            _ => Err(ValueError::ArrayTypeRequired.into()),
        }
    }
}
