use {
    super::ErrInto,
    gloo_utils::format::JsValueSerdeExt,
    gluesql_core::{
        ast::ColumnDef,
        data::Value,
        error::{Error, Result},
    },
    serde_json::Value as JsonValue,
    std::collections::BTreeMap,
    wasm_bindgen::JsValue,
};

pub fn js_value_to_row(value: JsValue, column_defs: Option<&[ColumnDef]>) -> Result<Vec<Value>> {
    let value: JsonValue = value.into_serde().err_into()?;

    match (value, column_defs) {
        (JsonValue::Array(json_array), Some(column_defs))
            if json_array.len() == column_defs.len() =>
        {
            json_array
                .into_iter()
                .map(Value::try_from)
                .zip(column_defs.iter())
                .map(|(value, ColumnDef { data_type, .. })| {
                    let value = value?;

                    match value.get_type() {
                        Some(curr_type) if &curr_type != data_type => value.cast(data_type),
                        _ => Ok(value),
                    }
                })
                .collect::<Result<Vec<_>>>()
        }
        (JsonValue::Object(json_map), None) => json_map
            .into_iter()
            .map(|(key, value)| value.try_into().map(|value| (key, value)))
            .collect::<Result<BTreeMap<String, Value>>>()
            .map(Value::Map)
            .map(|map| vec![map]),
        (value, _) => Err(Error::StorageMsg(format!(
            "conflict - unsupported value stored: {value:?}"
        ))),
    }
}

pub fn row_to_json_value(row: Vec<Value>, is_schemaless_table: bool) -> Result<JsonValue> {
    if !is_schemaless_table {
        return JsonValue::try_from(Value::List(row));
    }

    let mut values = row.into_iter();
    match (values.next(), values.next()) {
        (Some(Value::Map(values)), None) => JsonValue::try_from(Value::Map(values)),
        _ => Err(Error::StorageMsg(
            "conflict - expected schemaless row as [Map]".to_owned(),
        )),
    }
}
