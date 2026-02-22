use gluesql_core::{
    data::Point as CorePoint,
    prelude::{Payload as CorePayload, PayloadVariable, Value as CoreValue},
};
use std::collections::HashMap;

#[derive(uniffi::Enum)]
pub enum SqlValue {
    Bool { value: bool },
    I8 { value: i8 },
    I16 { value: i16 },
    I32 { value: i32 },
    I64 { value: i64 },
    U8 { value: u8 },
    U16 { value: u16 },
    U32 { value: u32 },
    F32 { value: f32 },
    F64 { value: f64 },
    BigInt { value: String },
    Str { value: String },
    Bytes { value: Vec<u8> },
    Inet { value: String },
    Date { value: String },
    Timestamp { value: String },
    Time { value: String },
    Interval { value: String },
    Uuid { value: String },
    SqlMap { value: HashMap<String, SqlValue> },
    SqlList { value: Vec<SqlValue> },
    SqlPoint { value: Point },
    Null,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}
impl From<CorePoint> for Point {
    fn from(p: CorePoint) -> Self {
        Self { x: p.x, y: p.y }
    }
}

impl From<CoreValue> for SqlValue {
    fn from(value: CoreValue) -> Self {
        match value {
            CoreValue::Bool(b) => SqlValue::Bool { value: b },
            CoreValue::I8(n) => SqlValue::I8 { value: n },
            CoreValue::I16(n) => SqlValue::I16 { value: n },
            CoreValue::I32(n) => SqlValue::I32 { value: n },
            CoreValue::I64(n) => SqlValue::I64 { value: n },
            CoreValue::I128(n) => SqlValue::BigInt {
                value: n.to_string(),
            },
            CoreValue::U8(n) => SqlValue::U8 { value: n },
            CoreValue::U16(n) => SqlValue::U16 { value: n },
            CoreValue::U32(n) => SqlValue::U32 { value: n },
            CoreValue::U64(n) => SqlValue::BigInt {
                value: n.to_string(),
            },
            CoreValue::U128(n) => SqlValue::BigInt {
                value: n.to_string(),
            },
            CoreValue::F32(f) => SqlValue::F32 { value: f },
            CoreValue::F64(f) => SqlValue::F64 { value: f },
            CoreValue::Decimal(d) => SqlValue::Str {
                value: d.to_string(),
            },
            CoreValue::Str(s) => SqlValue::Str { value: s },
            CoreValue::Bytea(bytes) => SqlValue::Bytes { value: bytes },
            CoreValue::Inet(ip) => SqlValue::Inet {
                value: ip.to_string(),
            },
            CoreValue::Date(date) => SqlValue::Date {
                value: date.format("%Y-%m-%d").to_string(),
            },
            CoreValue::Timestamp(ts) => SqlValue::Timestamp {
                value: ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            },
            CoreValue::Time(time) => SqlValue::Time {
                value: time.format("%H:%M:%S%.3f").to_string(),
            },
            CoreValue::Interval(interval) => SqlValue::Interval {
                value: format!("{:?}", interval),
            },
            CoreValue::Uuid(uuid) => {
                let uuid = uuid::Uuid::from_u128(uuid);
                SqlValue::Uuid {
                    value: uuid.to_string(),
                }
            }
            CoreValue::Map(map) => {
                let mut m: HashMap<String, SqlValue> = HashMap::new();
                map.into_iter().for_each(|(key, value)| {
                    m.insert(key, value.into());
                });
                SqlValue::SqlMap { value: m }
            }
            CoreValue::List(list) => SqlValue::SqlList {
                value: list.into_iter().map(|x| x.into()).collect(),
            },
            CoreValue::Point(point) => SqlValue::SqlPoint {
                value: point.into(),
            },
            CoreValue::Null => SqlValue::Null,
        }
    }
}

#[derive(uniffi::Enum)]
pub enum Payload {
    ShowColumns {
        columns: Vec<String>,
    },
    Create {
        rows: u64,
    },
    Insert {
        rows: u64,
    },
    Select {
        labels: Vec<String>,
        rows: Vec<Vec<SqlValue>>,
    },
    SelectMap {
        rows: Vec<std::collections::HashMap<String, SqlValue>>,
    },
    Delete {
        rows: u64,
    },
    Update {
        rows: u64,
    },
    DropTable {
        count: u64,
    },
    DropFunction,
    AlterTable,
    CreateIndex,
    DropIndex,
    StartTransaction,
    Commit,
    Rollback,
    ShowVariable {
        name: String,
        value: String,
    },
}

impl From<CorePayload> for Payload {
    fn from(payload: CorePayload) -> Self {
        match payload {
            CorePayload::ShowColumns(columns) => {
                let column_names: Vec<String> = columns.into_iter().map(|(name, _)| name).collect();
                Payload::ShowColumns {
                    columns: column_names,
                }
            }
            CorePayload::Create => Payload::Create { rows: 0 },
            CorePayload::Insert(rows) => Payload::Insert { rows: rows as u64 },
            CorePayload::Select { labels, rows } => {
                let converted_rows: Vec<Vec<SqlValue>> = rows
                    .into_iter()
                    .map(|row| row.into_iter().map(SqlValue::from).collect())
                    .collect();
                Payload::Select {
                    labels,
                    rows: converted_rows,
                }
            }
            CorePayload::SelectMap(rows) => {
                let converted_rows = rows
                    .into_iter()
                    .map(|map| {
                        map.into_iter()
                            .map(|(k, v)| (k, SqlValue::from(v)))
                            .collect()
                    })
                    .collect();
                Payload::SelectMap {
                    rows: converted_rows,
                }
            }
            CorePayload::Delete(rows) => Payload::Delete { rows: rows as u64 },
            CorePayload::Update(rows) => Payload::Update { rows: rows as u64 },
            CorePayload::DropTable(count) => Payload::DropTable {
                count: count as u64,
            },
            CorePayload::DropFunction => Payload::DropFunction,
            CorePayload::AlterTable => Payload::AlterTable,
            CorePayload::CreateIndex => Payload::CreateIndex,
            CorePayload::DropIndex => Payload::DropIndex,
            CorePayload::StartTransaction => Payload::StartTransaction,
            CorePayload::Commit => Payload::Commit,
            CorePayload::Rollback => Payload::Rollback,
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
                Payload::ShowVariable { name, value }
            }
        }
    }
}
