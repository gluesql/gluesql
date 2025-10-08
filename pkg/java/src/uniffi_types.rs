use std::collections::HashMap;
use gluesql_core::{
    data::{Point as CorePoint},
    prelude::{Payload as CorePayload, PayloadVariable, Value as CoreValue}
};

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
    Point { value: Point },
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
            CoreValue::I128(n) => SqlValue::BigInt { value: n.to_string() },
            CoreValue::U8(n) => SqlValue::U8 { value: n },
            CoreValue::U16(n) => SqlValue::U16 { value: n },
            CoreValue::U32(n) => SqlValue::U32 { value: n },
            CoreValue::U64(n) => SqlValue::BigInt { value: n.to_string() },
            CoreValue::U128(n) => SqlValue::BigInt { value: n.to_string() },
            CoreValue::F32(f) => SqlValue::F32 { value: f },
            CoreValue::F64(f) => SqlValue::F64 { value: f },
            CoreValue::Decimal(d) => SqlValue::Str { value: d.to_string() },
            CoreValue::Str(s) => SqlValue::Str { value: s },
            CoreValue::Bytea(bytes) => SqlValue::Bytes { value: bytes },
            CoreValue::Inet(ip) => SqlValue::Inet { value: ip.to_string() },
            CoreValue::Date(date) => SqlValue::Date { value: date.format("%Y-%m-%d").to_string() },
            CoreValue::Timestamp(ts) => SqlValue::Timestamp { value: ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string() },
            CoreValue::Time(time) => SqlValue::Time { value: time.format("%H:%M:%S%.3f").to_string() },
            CoreValue::Interval(interval) => SqlValue::Interval { value: format!("{:?}", interval) },
            CoreValue::Uuid(uuid) => {
                let uuid = uuid::Uuid::from_u128(uuid);
                SqlValue::Uuid { value: uuid.to_string() }
            }
            CoreValue::Map(map) => {
                let mut m: HashMap<String, SqlValue> = HashMap::new();
                map.into_iter().for_each(|(key, value)| {
                    m.insert(key, value.into());
                });
                SqlValue::SqlMap { value: m }
            }
            CoreValue::List(list) => SqlValue::SqlList { value: list.into_iter().map(|x| x.into()).collect() },
            CoreValue::Point(point) => SqlValue::Point { value: point.into() },
            CoreValue::Null => SqlValue::Null,
        }
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
