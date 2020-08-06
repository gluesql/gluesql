use serde_json::value::Value as Json;
use wasm_bindgen::prelude::JsValue;

use gluesql::{Payload, Row, Value};

pub fn convert(payloads: Vec<Payload>) -> JsValue {
    let payloads = Json::Array(payloads.into_iter().map(convert_payload).collect());

    JsValue::from_serde(&payloads).unwrap()
}

fn convert_payload(payload: Payload) -> Json {
    match payload {
        Payload::Create => Json::Null,
        Payload::Insert(row) => convert_row(row),
        Payload::Select(rows) => Json::Array(rows.into_iter().map(convert_row).collect()),
        Payload::Delete(num) => Json::from(num),
        Payload::Update(num) => Json::from(num),
        Payload::DropTable => Json::Null,
    }
}

fn convert_row(row: Row) -> Json {
    let Row(values) = row;

    Json::Array(values.into_iter().map(convert_value).collect())
}

fn convert_value(value: Value) -> Json {
    use Value::*;

    match value {
        Bool(v) | OptBool(Some(v)) => Json::Bool(v),
        I64(v) | OptI64(Some(v)) => Json::from(v),
        F64(v) | OptF64(Some(v)) => Json::from(v),
        Str(v) | OptStr(Some(v)) => Json::String(v),
        OptBool(None) | OptI64(None) | OptF64(None) | OptStr(None) | Empty => Json::Null,
    }
}
