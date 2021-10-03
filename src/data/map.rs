use {
    crate::{data::Value, result::Result},
    serde::{Deserialize, Serialize},
    serde_json::{Map as JsonMap, Value as JsonValue},
    std::{collections::HashMap, fmt::Debug, ops::ControlFlow},
    thiserror::Error as ThisError,
};

#[derive(Debug, PartialEq, ThisError, Serialize)]
pub enum MapError {
    #[error("parse failed - invalid json")]
    InvalidJsonString,

    #[error("parse failed - json object type is required")]
    ObjectTypeJsonRequired,

    #[error("Array type is not yet supported")]
    ArrayTypeNotSupported,

    #[error("unreachable - failed to parse json number value: {0}")]
    UnreachableJsonNumberParseFailure(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Map(HashMap<String, Value>);

impl Map {
    pub fn parse_json(value: &str) -> Result<Self> {
        let value: JsonValue =
            serde_json::from_str(value).map_err(|_| MapError::InvalidJsonString)?;

        match value {
            JsonValue::Object(json_map) => parse_json_map(json_map),
            _ => Err(MapError::ObjectTypeJsonRequired.into()),
        }
    }

    pub fn get_value(&self, selector: &str) -> Value {
        let splitted = selector.split('.').collect::<Vec<_>>();
        let size = splitted.len();
        let result = splitted
            .into_iter()
            .enumerate()
            .map(|(i, key)| (i == size - 1, key))
            .try_fold(&self.0, |map, (is_last, key)| {
                let value = match map.get(key) {
                    Some(value) => value,
                    None => {
                        return ControlFlow::Break(Value::Null);
                    }
                };

                match (is_last, value) {
                    (true, value) => ControlFlow::Break(value.clone()),
                    (false, Value::Map(map)) => ControlFlow::Continue(&map.0),
                    (false, _) => ControlFlow::Break(Value::Null),
                }
            });

        match result {
            ControlFlow::Continue(map) => Value::Map(Self(map.clone())),
            ControlFlow::Break(value) => value,
        }
    }
}

fn parse_json_map(json_map: JsonMap<String, JsonValue>) -> Result<Map> {
    json_map
        .into_iter()
        .map(|(key, value)| parse_json_value(value).map(|value| (key, value)))
        .collect::<Result<HashMap<String, Value>>>()
        .map(Map)
}

fn parse_json_value(json_value: JsonValue) -> Result<Value> {
    match json_value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(v) => Ok(Value::Bool(v)),
        JsonValue::Number(v) => {
            if let Some(value) = v.as_i64().map(Value::I64) {
                return Ok(value);
            }

            v.as_f64()
                .map(Value::F64)
                .ok_or_else(|| MapError::UnreachableJsonNumberParseFailure(v.to_string()).into())
        }
        JsonValue::String(v) => Ok(Value::Str(v)),
        JsonValue::Array(_) => Err(MapError::ArrayTypeNotSupported.into()),
        JsonValue::Object(json_map) => parse_json_map(json_map).map(Value::Map),
    }
}
