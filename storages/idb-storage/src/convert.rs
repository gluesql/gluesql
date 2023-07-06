use {
    super::ErrInto,
    gloo_utils::format::JsValueSerdeExt,
    gluesql_core::{
        ast::ColumnDef,
        data::Value,
        error::{Error, Result},
        store::DataRow,
    },
    serde_json::Value as JsonValue,
    std::collections::HashMap,
    wasm_bindgen::JsValue,
};

pub fn convert(value: JsValue, column_defs: Option<&[ColumnDef]>) -> Result<DataRow> {
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
                .map(DataRow::Vec)
        }
        (JsonValue::Object(json_map), None) => json_map
            .into_iter()
            .map(|(key, value)| value.try_into().map(|value| (key, value)))
            .collect::<Result<HashMap<String, Value>>>()
            .map(DataRow::Map),
        (value, _) => Err(Error::StorageMsg(format!(
            "conflict - unsupported value stored: {value:?}"
        ))),
    }
}
