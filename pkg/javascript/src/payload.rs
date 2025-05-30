#![cfg(target_arch = "wasm32")]

use {
    gloo_utils::format::JsValueSerdeExt,
    gluesql_core::prelude::{Payload, PayloadVariable},
    serde_json::{Value as Json, json},
    wasm_bindgen::prelude::JsValue,
};

pub fn convert(payloads: Vec<Payload>) -> JsValue {
    let payloads = payloads.into_iter().map(convert_payload).collect();
    let payloads = Json::Array(payloads);

    JsValue::from_serde(&payloads).unwrap()
}

fn convert_payload(payload: Payload) -> Json {
    match payload {
        Payload::Create => json!({ "type": "CREATE TABLE" }),
        Payload::DropTable(num) => json!({ "type": "DROP TABLE", "affected": num }),
        Payload::Select { labels, rows } => {
            let rows = rows
                .into_iter()
                .map(|values| {
                    let row = labels
                        .iter()
                        .zip(values.into_iter())
                        .map(|(label, value)| {
                            let key = label.to_owned();
                            let value = Json::try_from(value).unwrap();

                            (key, value)
                        })
                        .collect();

                    Json::Object(row)
                })
                .collect();

            json!({
                "type": "SELECT",
                "rows": Json::Array(rows),
            })
        }
        Payload::SelectMap(rows) => {
            let rows = rows
                .into_iter()
                .map(|row| {
                    let row = row
                        .into_iter()
                        .map(|(key, value)| {
                            let value = Json::try_from(value).unwrap();

                            (key, value)
                        })
                        .collect();

                    Json::Object(row)
                })
                .collect();

            json!({
                "type": "SELECT",
                "rows": Json::Array(rows),
            })
        }
        Payload::ShowColumns(columns) => {
            let columns = columns
                .into_iter()
                .map(|(name, data_type)| {
                    json!({
                        "name": name,
                        "type": data_type.to_string(),
                    })
                })
                .collect();

            json!({
                "type": "SHOW COLUMNS",
                "columns": Json::Array(columns),
            })
        }
        Payload::Insert(num) => json!({
            "type": "INSERT",
            "affected": num
        }),
        Payload::Update(num) => json!({
            "type": "UPDATE",
            "affected": num
        }),
        Payload::Delete(num) => json!({
            "type": "DELETE",
            "affected": num
        }),
        Payload::AlterTable => json!({ "type": "ALTER TABLE" }),
        Payload::CreateIndex => json!({ "type": "CREATE INDEX" }),
        Payload::DropIndex => json!({ "type": "DROP INDEX" }),
        Payload::StartTransaction => json!({ "type": "BEGIN" }),
        Payload::Commit => json!({ "type": "COMMIT" }),
        Payload::Rollback => json!({ "type": "ROLLBACK" }),
        Payload::ShowVariable(PayloadVariable::Version(version)) => {
            json!({
                "type": "SHOW VERSION",
                "version": version
            })
        }
        Payload::ShowVariable(PayloadVariable::Tables(table_names)) => {
            json!({
                "type": "SHOW TABLES",
                "tables": table_names
            })
        }
        Payload::DropFunction => json!({ "type": "DROP FUNCTION" }),
        Payload::ShowVariable(PayloadVariable::Functions(function_names)) => {
            json!({
                "type": "SHOW FUNCTIONS",
                "functions": function_names
            })
        }
    }
}
