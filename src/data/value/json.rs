use {
    super::{Value, ValueError},
    crate::result::Result,
    serde_json::Value as JsonValue,
    std::collections::HashMap,
};

impl Value {
    pub fn parse_json_map(value: &str) -> Result<Value> {
        let value: JsonValue =
            serde_json::from_str(value).map_err(|_| ValueError::InvalidJsonString)?;

        if !matches!(value, JsonValue::Object(_)) {
            return Err(ValueError::JsonObjectTypeRequired.into());
        }

        parse_json_value(value)
    }

    pub fn parse_json_list(value: &str) -> Result<Value> {
        let value: JsonValue =
            serde_json::from_str(value).map_err(|_| ValueError::InvalidJsonString)?;

        if !matches!(value, JsonValue::Array(_)) {
            return Err(ValueError::JsonArrayTypeRequired.into());
        }

        parse_json_value(value)
    }
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
                .ok_or_else(|| ValueError::UnreachableJsonNumberParseFailure(v.to_string()).into())
        }
        JsonValue::String(v) => Ok(Value::Str(v)),
        JsonValue::Array(json_array) => json_array
            .into_iter()
            .map(parse_json_value)
            .collect::<Result<Vec<Value>>>()
            .map(Value::List),
        JsonValue::Object(json_map) => json_map
            .into_iter()
            .map(|(key, value)| parse_json_value(value).map(|value| (key, value)))
            .collect::<Result<HashMap<String, Value>>>()
            .map(Value::Map),
    }
}
