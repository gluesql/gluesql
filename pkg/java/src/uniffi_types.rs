use gluesql_core::prelude::{Payload as CorePayload, PayloadVariable, Value as CoreValue};

#[derive(uniffi::Enum)]
pub enum SqlValue {
    Bool { value: bool },
    I64 { value: i64 },
    F64 { value: f64 },
    Str { value: String },
    Bytes { value: Vec<u8> },
    Null,
}

impl From<CoreValue> for SqlValue {
    fn from(value: CoreValue) -> Self {
        match value {
            CoreValue::Bool(b) => SqlValue::Bool { value: b },
            CoreValue::I8(n) => SqlValue::I64 { value: n as i64 },
            CoreValue::I16(n) => SqlValue::I64 { value: n as i64 },
            CoreValue::I32(n) => SqlValue::I64 { value: n as i64 },
            CoreValue::I64(n) => SqlValue::I64 { value: n },
            CoreValue::I128(n) => SqlValue::I64 { value: n as i64 }, // Truncation risk
            CoreValue::U8(n) => SqlValue::I64 { value: n as i64 },
            CoreValue::U16(n) => SqlValue::I64 { value: n as i64 },
            CoreValue::U32(n) => SqlValue::I64 { value: n as i64 },
            CoreValue::U64(n) => SqlValue::I64 { value: n as i64 }, // Truncation risk
            CoreValue::U128(n) => SqlValue::I64 { value: n as i64 }, // Truncation risk
            CoreValue::F32(f) => SqlValue::F64 { value: f as f64 },
            CoreValue::F64(f) => SqlValue::F64 { value: f },
            CoreValue::Decimal(d) => SqlValue::Str {
                value: d.to_string(),
            },
            CoreValue::Str(s) => SqlValue::Str { value: s },
            CoreValue::Bytea(bytes) => SqlValue::Bytes { value: bytes },
            CoreValue::Inet(ip) => SqlValue::Str {
                value: ip.to_string(),
            },
            CoreValue::Date(date) => SqlValue::Str {
                value: date.format("%Y-%m-%d").to_string(),
            },
            CoreValue::Timestamp(ts) => SqlValue::Str {
                value: ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            },
            CoreValue::Time(time) => SqlValue::Str {
                value: time.format("%H:%M:%S%.3f").to_string(),
            },
            CoreValue::Interval(interval) => SqlValue::Str {
                value: format!("{:?}", interval),
            },
            CoreValue::Uuid(uuid) => {
                let uuid = uuid::Uuid::from_u128(uuid);
                SqlValue::Str {
                    value: uuid.to_string(),
                }
            }
            CoreValue::Map(map) => {
                // Convert to JSON string representation
                let json_map: serde_json::Map<String, serde_json::Value> = map
                    .into_iter()
                    .map(|(k, v)| (k, convert_value_to_json(v)))
                    .collect();
                SqlValue::Str {
                    value: serde_json::to_string(&json_map).unwrap_or_else(|_| "{}".to_string()),
                }
            }
            CoreValue::List(list) => {
                // Convert to JSON array representation
                let json_list: Vec<serde_json::Value> =
                    list.into_iter().map(convert_value_to_json).collect();
                SqlValue::Str {
                    value: serde_json::to_string(&json_list).unwrap_or_else(|_| "[]".to_string()),
                }
            }
            CoreValue::Point(point) => SqlValue::Str {
                value: format!("({}, {})", point.x, point.y),
            },
            CoreValue::Null => SqlValue::Null,
        }
    }
}

fn convert_value_to_json(value: CoreValue) -> serde_json::Value {
    use serde_json::Value as JsonValue;

    match value {
        CoreValue::Bool(b) => JsonValue::Bool(b),
        CoreValue::I8(n) => JsonValue::Number(serde_json::Number::from(n)),
        CoreValue::I16(n) => JsonValue::Number(serde_json::Number::from(n)),
        CoreValue::I32(n) => JsonValue::Number(serde_json::Number::from(n)),
        CoreValue::I64(n) => JsonValue::Number(serde_json::Number::from(n)),
        CoreValue::I128(n) => JsonValue::String(n.to_string()),
        CoreValue::U8(n) => JsonValue::Number(serde_json::Number::from(n)),
        CoreValue::U16(n) => JsonValue::Number(serde_json::Number::from(n)),
        CoreValue::U32(n) => JsonValue::Number(serde_json::Number::from(n)),
        CoreValue::U64(n) => JsonValue::Number(serde_json::Number::from(n)),
        CoreValue::U128(n) => JsonValue::String(n.to_string()),
        CoreValue::F32(f) => serde_json::Number::from_f64(f as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::String(f.to_string())),
        CoreValue::F64(f) => serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::String(f.to_string())),
        CoreValue::Decimal(d) => JsonValue::String(d.to_string()),
        CoreValue::Str(s) => JsonValue::String(s),
        CoreValue::Bytea(bytes) => JsonValue::String(format!("\\x{}", hex::encode(bytes))),
        CoreValue::Inet(ip) => JsonValue::String(ip.to_string()),
        CoreValue::Date(date) => JsonValue::String(date.format("%Y-%m-%d").to_string()),
        CoreValue::Timestamp(ts) => {
            JsonValue::String(ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
        }
        CoreValue::Time(time) => JsonValue::String(time.format("%H:%M:%S%.3f").to_string()),
        CoreValue::Interval(interval) => JsonValue::String(format!("{:?}", interval)),
        CoreValue::Uuid(uuid) => {
            let uuid = uuid::Uuid::from_u128(uuid);
            JsonValue::String(uuid.to_string())
        }
        CoreValue::Map(map) => {
            let json_map: serde_json::Map<String, JsonValue> = map
                .into_iter()
                .map(|(k, v)| (k, convert_value_to_json(v)))
                .collect();
            JsonValue::Object(json_map)
        }
        CoreValue::List(list) => {
            let json_list: Vec<JsonValue> = list.into_iter().map(convert_value_to_json).collect();
            JsonValue::Array(json_list)
        }
        CoreValue::Point(point) => {
            let mut map = serde_json::Map::new();
            map.insert(
                "x".to_string(),
                JsonValue::Number(serde_json::Number::from_f64(point.x).unwrap()),
            );
            map.insert(
                "y".to_string(),
                JsonValue::Number(serde_json::Number::from_f64(point.y).unwrap()),
            );
            JsonValue::Object(map)
        }
        CoreValue::Null => JsonValue::Null,
    }
}

#[derive(uniffi::Record)]
pub struct SelectResult {
    pub rows: Vec<Vec<SqlValue>>,
    pub labels: Vec<String>,
}

#[derive(uniffi::Record)]
pub struct CreateResult {
    pub rows: u64,
}

#[derive(uniffi::Record)]
pub struct InsertResult {
    pub rows: u64,
}

#[derive(uniffi::Record)]
pub struct UpdateResult {
    pub rows: u64,
}

#[derive(uniffi::Record)]
pub struct DeleteResult {
    pub rows: u64,
}

#[derive(uniffi::Record)]
pub struct DropTableResult {
    pub count: u64,
}

#[derive(uniffi::Record)]
pub struct ShowVariableResult {
    pub name: String,
    pub value: String,
}

#[derive(uniffi::Record)]
pub struct ShowColumnsResult {
    pub columns: Vec<String>,
}

#[derive(uniffi::Enum)]
pub enum QueryResult {
    Create { result: CreateResult },
    Insert { result: InsertResult },
    Update { result: UpdateResult },
    Delete { result: DeleteResult },
    Select { result: SelectResult },
    DropTable { result: DropTableResult },
    AlterTable,
    StartTransaction,
    Commit,
    Rollback,
    ShowVariable { result: ShowVariableResult },
    ShowColumns { result: ShowColumnsResult },
}

impl From<CorePayload> for QueryResult {
    fn from(payload: CorePayload) -> Self {
        match payload {
            CorePayload::Create => QueryResult::Create {
                result: CreateResult { rows: 0 },
            },
            CorePayload::Insert(rows) => QueryResult::Insert {
                result: InsertResult { rows: rows as u64 },
            },
            CorePayload::Update(rows) => QueryResult::Update {
                result: UpdateResult { rows: rows as u64 },
            },
            CorePayload::Delete(rows) => QueryResult::Delete {
                result: DeleteResult { rows: rows as u64 },
            },
            CorePayload::Select { labels, rows } => {
                let converted_rows: Vec<Vec<SqlValue>> = rows
                    .into_iter()
                    .map(|row| row.into_iter().map(SqlValue::from).collect())
                    .collect();
                QueryResult::Select {
                    result: SelectResult {
                        rows: converted_rows,
                        labels,
                    },
                }
            }
            CorePayload::DropTable(count) => QueryResult::DropTable {
                result: DropTableResult {
                    count: count as u64,
                },
            },
            CorePayload::AlterTable => QueryResult::AlterTable,
            CorePayload::StartTransaction => QueryResult::StartTransaction,
            CorePayload::Commit => QueryResult::Commit,
            CorePayload::Rollback => QueryResult::Rollback,
            CorePayload::ShowVariable(var) => {
                let (name, value) = match var {
                    PayloadVariable::Tables(tables) => {
                        ("TABLES".to_string(), format!("{:?}", tables))
                    }
                    PayloadVariable::Functions(functions) => {
                        ("FUNCTIONS".to_string(), format!("{:?}", functions))
                    }
                    PayloadVariable::Version(version) => ("VERSION".to_string(), version),
                };
                QueryResult::ShowVariable {
                    result: ShowVariableResult { name, value },
                }
            }
            CorePayload::ShowColumns(columns) => {
                let column_names: Vec<String> = columns.into_iter().map(|(name, _)| name).collect();
                QueryResult::ShowColumns {
                    result: ShowColumnsResult {
                        columns: column_names,
                    },
                }
            }
            _ => QueryResult::AlterTable, // Handle other variants
        }
    }
}
