use {
    gluesql_core::prelude::{Payload, PayloadVariable},
    pyo3::{pyclass, PyObject, Python},
    pythonize::pythonize,
    serde_json::{json, Value as Json},
};

#[pyclass]
pub struct PyPayload {
    pub payload: Payload,
}

pub fn convert(py: Python, payloads: Vec<PyPayload>) -> PyObject {
    let payloads = payloads
        .into_iter()
        .map(|var| convert_payload(var.payload))
        .collect();
    let payloads = Json::Array(payloads);

    pythonize(py, &payloads).unwrap()
}

fn convert_payload(payload: Payload) -> Json {
    // TODO: Improve below logic.
    match payload {
        Payload::Create => json!({ "type": "CREATE TABLE" }),
        Payload::DropTable => json!({ "type": "DROP TABLE" }),
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
