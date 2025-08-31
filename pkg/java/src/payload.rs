use gluesql_core::prelude::{Payload as CorePayload, PayloadVariable};

#[derive(serde::Serialize)]
pub enum Payload {
    Create {
        rows: u64,
    },
    Insert {
        rows: u64,
    },
    Update {
        rows: u64,
    },
    Delete {
        rows: u64,
    },
    Select {
        rows: Vec<Vec<String>>,
        labels: Vec<String>,
    },
    DropTable {
        count: u64,
    },
    AlterTable,
    StartTransaction,
    Commit,
    Rollback,
    ShowVariable {
        name: String,
        value: String,
    },
    ShowColumns {
        columns: Vec<String>,
    },
}

pub fn convert_payload(payload: CorePayload) -> Payload {
    match payload {
        CorePayload::Create => Payload::Create { rows: 0 },
        CorePayload::Insert(rows) => Payload::Insert { rows: rows as u64 },
        CorePayload::Update(rows) => Payload::Update { rows: rows as u64 },
        CorePayload::Delete(rows) => Payload::Delete { rows: rows as u64 },
        CorePayload::Select { labels, rows } => {
            let converted_rows: Vec<Vec<String>> = rows
                .into_iter()
                .map(|row| row.into_iter().map(|value| convert_value(value)).collect())
                .collect();
            Payload::Select {
                rows: converted_rows,
                labels,
            }
        }
        CorePayload::DropTable(count) => Payload::DropTable {
            count: count as u64,
        },
        CorePayload::AlterTable => Payload::AlterTable,
        CorePayload::StartTransaction => Payload::StartTransaction,
        CorePayload::Commit => Payload::Commit,
        CorePayload::Rollback => Payload::Rollback,
        CorePayload::ShowVariable(var) => {
            let (name, value) = match var {
                PayloadVariable::Tables(tables) => ("TABLES".to_string(), format!("{:?}", tables)),
                PayloadVariable::Functions(functions) => {
                    ("FUNCTIONS".to_string(), format!("{:?}", functions))
                }
                PayloadVariable::Version(version) => ("VERSION".to_string(), version),
            };
            Payload::ShowVariable { name, value }
        }
        CorePayload::ShowColumns(columns) => {
            let column_names: Vec<String> = columns.into_iter().map(|(name, _)| name).collect();
            Payload::ShowColumns {
                columns: column_names,
            }
        }
        _ => Payload::AlterTable, // Handle other variants
    }
}

fn convert_value(value: gluesql_core::prelude::Value) -> String {
    let debug_str = format!("{:?}", value);
    // Remove Rust type prefixes like I64(), Str(), etc.
    if debug_str.starts_with("I64(") && debug_str.ends_with(")") {
        debug_str[4..debug_str.len() - 1].to_string()
    } else if debug_str.starts_with("Str(") && debug_str.ends_with(")") {
        let inner = &debug_str[5..debug_str.len() - 2]; // Remove Str(" and ")
        inner.to_string()
    } else if debug_str.starts_with("F64(") && debug_str.ends_with(")") {
        debug_str[4..debug_str.len() - 1].to_string()
    } else if debug_str.starts_with("Bool(") && debug_str.ends_with(")") {
        debug_str[5..debug_str.len() - 1].to_string()
    } else if debug_str == "Null" {
        "null".to_string()
    } else {
        debug_str
    }
}
