use {
    gloo_utils::format::JsValueSerdeExt,
    gluesql_core::{ast::ColumnDef, data::Value, result::Result, store::DataRow},
    serde_json::Value as JsonValue,
    std::collections::HashMap,
    wasm_bindgen::JsValue,
};

pub fn convert(value: JsValue, column_defs: Option<&[ColumnDef]>) -> Result<DataRow> {
    let value: JsonValue = value.into_serde().unwrap();
    // let value: JsonValue = serde_json::to_value(value.into_serde::<().unwrap()).unwrap();

    match (value, column_defs) {
        (JsonValue::Array(json_array), Some(column_defs)) => json_array
            .into_iter()
            .map(Value::try_from)
            .zip(column_defs.iter())
            .map(|(value, ColumnDef { data_type, .. })| {
                let value = value?;

                match value.get_type() {
                    Some(curr_type) if &curr_type != data_type => value.cast(&data_type),
                    _ => Ok(value),
                }
            })
            .collect::<Result<Vec<Value>>>()
            .map(DataRow::Vec),
        (JsonValue::Object(json_map), None) => json_map
            .into_iter()
            .map(|(key, value)| value.try_into().map(|value| (key, value)))
            .collect::<Result<HashMap<String, Value>>>()
            .map(DataRow::Map),
        (v, _) => {
            Ok(DataRow::Vec(vec![]))
            // todo!("{v:?}");
        }
    }
}

// DataRow to JsValue..? no Value
