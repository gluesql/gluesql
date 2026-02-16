#![cfg(target_arch = "wasm32")]

use {
    gloo_utils::format::JsValueSerdeExt,
    gluesql_core::{
        ast::{ColumnDef, DataType},
        data::Value,
        error::Error,
    },
    gluesql_idb_storage::convert::{js_value_to_row, row_to_json_value},
    serde_json::json,
    std::collections::BTreeMap,
    wasm_bindgen::JsValue,
    wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure},
};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn convert_schema() {
    let actual_data = json!([100, "hello", true]);
    let actual_data = JsValue::from_serde(&actual_data).unwrap();
    let actual_defs = vec![
        ColumnDef {
            name: "id".to_owned(),
            data_type: DataType::Int8,
            nullable: false,
            default: None,
            unique: None,
            comment: None,
        },
        ColumnDef {
            name: "name".to_owned(),
            data_type: DataType::Text,
            nullable: false,
            default: None,
            unique: None,
            comment: None,
        },
        ColumnDef {
            name: "flag".to_owned(),
            data_type: DataType::Boolean,
            nullable: true,
            default: None,
            unique: None,
            comment: None,
        },
    ];
    let expected = vec![
        Value::I8(100),
        Value::Str("hello".to_owned()),
        Value::Bool(true),
    ];
    assert_eq!(
        js_value_to_row(actual_data, Some(actual_defs.as_slice())),
        Ok(expected)
    );
}

#[wasm_bindgen_test]
async fn convert_schemaless() {
    let actual = json!({
        "id": 100,
        "name": "hello",
        "flag": true
    });
    let actual = JsValue::from_serde(&actual).unwrap();
    let expected = vec![Value::Map(
        [
            ("id".to_owned(), Value::I64(100)),
            ("name".to_owned(), Value::Str("hello".to_owned())),
            ("flag".to_owned(), Value::Bool(true)),
        ]
        .into_iter()
        .collect(),
    )];
    assert_eq!(js_value_to_row(actual, None), Ok(expected));
}

#[wasm_bindgen_test]
async fn convert_row_to_json_value_schema() {
    let row = vec![
        Value::I8(100),
        Value::Str("hello".to_owned()),
        Value::Bool(true),
    ];
    let actual = row_to_json_value(row, false);
    let expected = Ok(json!([100, "hello", true]));
    assert_eq!(actual, expected);
}

#[wasm_bindgen_test]
async fn convert_row_to_json_value_schemaless() {
    let row = vec![Value::Map(BTreeMap::from([
        ("id".to_owned(), Value::I64(100)),
        ("name".to_owned(), Value::Str("hello".to_owned())),
        ("flag".to_owned(), Value::Bool(true)),
    ]))];

    let actual = row_to_json_value(row, true);
    let expected = Ok(json!({
        "id": 100,
        "name": "hello",
        "flag": true
    }));
    assert_eq!(actual, expected);
}

#[wasm_bindgen_test]
async fn convert_row_to_json_value_rejects_invalid_schemaless_shape() {
    let row = vec![Value::I64(100)];

    let actual = row_to_json_value(row, true);
    let expected = Err(Error::StorageMsg(
        "conflict - expected schemaless row as [Map]".to_owned(),
    ));
    assert_eq!(actual, expected);
}
