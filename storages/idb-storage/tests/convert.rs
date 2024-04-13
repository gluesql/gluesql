#![cfg(target_arch = "wasm32")]

use {
    gloo_utils::format::JsValueSerdeExt,
    gluesql_core::{
        ast::{ColumnDef, DataType},
        data::Value,
        store::DataRow,
    },
    gluesql_idb_storage::convert::convert,
    serde_json::json,
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
    let expected = DataRow::Vec(vec![
        Value::I8(100),
        Value::Str("hello".to_owned()),
        Value::Bool(true),
    ]);
    assert_eq!(
        convert(actual_data, Some(actual_defs.as_slice())),
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
    let expected = DataRow::Map(
        [
            ("id".to_owned(), Value::I64(100)),
            ("name".to_owned(), Value::Str("hello".to_owned())),
            ("flag".to_owned(), Value::Bool(true)),
        ]
        .into_iter()
        .collect(),
    );
    assert_eq!(convert(actual, None), Ok(expected));
}
