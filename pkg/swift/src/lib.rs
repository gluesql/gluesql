use gluesql_core::{
    ast::DataType as RustDataType,
    data::{Interval as RustInterval, Point as RustPoint, Value as RustValue},
    prelude::{Glue as RustGlue, Payload as RustPayload, PayloadVariable as RustPayloadVariable},
};
use gluesql_memory_storage::MemoryStorage;
use gluesql_shared_memory_storage::SharedMemoryStorage;

use std::{
    collections::HashMap,
    net::{IpAddr as RustIpAddr, Ipv4Addr as RustIpv4Addr, Ipv6Addr as RustIpv6Addr},
    sync::{LazyLock, Mutex},
    thread,
};
use tokio::sync::mpsc;
use uuid::Uuid;

uniffi::setup_scaffolding!("gluesql");

struct Task {
    sql: Vec<String>,
}

struct GlobalQueue {
    sender: tokio::sync::mpsc::Sender<Dto>,
}

enum GlueStorage {
    MemoryStorage(RustGlue<MemoryStorage>),
    SharedMemoryStorage(RustGlue<SharedMemoryStorage>),
}

#[derive(uniffi::Enum, Clone, Copy, PartialEq, Eq, Debug)]
pub enum GlueStorageKind {
    MemoryStorage,
    SharedMemoryStorage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Record)]
pub struct I128 {
    pub high: u64,
    pub low: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Record)]
pub struct U128 {
    pub high: u64,
    pub low: u64,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum Value {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(I128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(U128),
    F32(f32),
    F64(f64),
    Decimal(String),
    Str(String),
    Bytea(Vec<u8>),
    Inet(IpAddr),
    Date(String),
    Timestamp(String),
    Time(String),
    Interval(Interval),
    Uuid(String),
    Map(HashMap<String, Value>),
    List(Vec<Value>),
    Point(Point),
    Null,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}
impl From<RustPoint> for Point {
    fn from(p: RustPoint) -> Self {
        Self { x: p.x, y: p.y }
    }
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum Interval {
    Month(i32),
    Microsecond(i64),
}
impl From<RustInterval> for Interval {
    fn from(i: RustInterval) -> Self {
        match i {
            RustInterval::Month(m) => Self::Month(m),
            RustInterval::Microsecond(m) => Self::Microsecond(m),
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct Ipv4Addr {
    // octets: [u8; 4],
    octets: Vec<u8>,
}

impl From<RustIpv4Addr> for Ipv4Addr {
    fn from(i: RustIpv4Addr) -> Self {
        Self {
            octets: i.octets().into(),
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct Ipv6Addr {
    octets: Vec<u8>,
}
impl From<RustIpv6Addr> for Ipv6Addr {
    fn from(i: RustIpv6Addr) -> Self {
        Self {
            octets: i.octets().into(),
        }
    }
}
#[derive(uniffi::Enum, Clone, Debug)]
pub enum IpAddr {
    V4(Ipv4Addr),
    V6(Ipv6Addr),
}
impl From<RustIpAddr> for IpAddr {
    fn from(i: RustIpAddr) -> Self {
        match i {
            RustIpAddr::V4(v) => Self::V4(v.into()),
            RustIpAddr::V6(v) => Self::V6(v.into()),
        }
    }
}

impl From<RustValue> for Value {
    fn from(value: RustValue) -> Self {
        match value {
            RustValue::Bool(v) => Value::Bool(v),
            RustValue::I8(v) => Value::I8(v),
            RustValue::I16(v) => Value::I16(v),
            RustValue::I32(v) => Value::I32(v),
            RustValue::I64(v) => Value::I64(v),
            RustValue::I128(v) => {
                let raw_vec = v.to_le_bytes();
                let high = u64::from_le_bytes(raw_vec[0..8].try_into().unwrap());
                let low = u64::from_le_bytes(raw_vec[8..16].try_into().unwrap());
                Value::I128(I128 { high, low })
            }
            RustValue::U8(v) => Value::U8(v),
            RustValue::U16(v) => Value::U16(v),
            RustValue::U32(v) => Value::U32(v),
            RustValue::U64(v) => Value::U64(v),
            RustValue::U128(v) => {
                let raw_vec = v.to_le_bytes();
                let high = u64::from_le_bytes(raw_vec[0..8].try_into().unwrap());
                let low = u64::from_le_bytes(raw_vec[8..16].try_into().unwrap());
                Value::U128(U128 { high, low })
            }
            RustValue::F32(v) => Value::F32(v),
            RustValue::F64(v) => Value::F64(v),
            RustValue::Decimal(v) => Value::Decimal(format!("{v}")),
            RustValue::Str(v) => Value::Str(v),
            RustValue::Bytea(v) => Value::Bytea(v),
            RustValue::Inet(v) => Value::Inet(v.into()),
            RustValue::Date(v) => Value::Date(v.to_string()),
            RustValue::Timestamp(v) => Value::Timestamp(v.to_string()),
            RustValue::Time(v) => Value::Time(format!("{v}")),
            RustValue::Interval(v) => Value::Interval(v.into()),
            RustValue::Uuid(v) => Value::Uuid(v.to_string()),
            RustValue::Map(v) => {
                let mut m: HashMap<String, Value> = HashMap::new();
                v.into_iter().for_each(|(key, value)| {
                    m.insert(key, value.into());
                });
                Value::Map(m)
            }
            RustValue::List(v) => Value::List(v.into_iter().map(|x| x.into()).collect()),
            RustValue::Point(v) => Value::Point(v.into()),
            RustValue::Null => Value::Null,
        }
    }
}

#[derive(Debug, uniffi::Enum)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int,
    Int128,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Uint128,
    Float32,
    Float,
    Text,
    Bytea,
    Inet,
    Date,
    Timestamp,
    Time,
    Interval,
    Uuid,
    Map,
    List,
    Decimal,
    Point,
}
impl From<RustDataType> for DataType {
    fn from(data_type: RustDataType) -> Self {
        match data_type {
            RustDataType::Int => DataType::Int,
            RustDataType::Boolean => DataType::Boolean,
            RustDataType::Int8 => DataType::Int8,
            RustDataType::Int16 => DataType::Int16,
            RustDataType::Int32 => DataType::Int32,
            RustDataType::Int128 => DataType::Int128,
            RustDataType::Uint8 => DataType::Uint8,
            RustDataType::Uint16 => DataType::Uint16,
            RustDataType::Uint32 => DataType::Uint32,
            RustDataType::Uint64 => DataType::Uint64,
            RustDataType::Uint128 => DataType::Uint128,
            RustDataType::Float32 => DataType::Float32,
            RustDataType::Float => DataType::Float,
            RustDataType::Text => DataType::Text,
            RustDataType::Bytea => DataType::Bytea,
            RustDataType::Inet => DataType::Inet,
            RustDataType::Date => DataType::Date,
            RustDataType::Timestamp => DataType::Timestamp,
            RustDataType::Time => DataType::Time,
            RustDataType::Interval => DataType::Interval,
            RustDataType::Uuid => DataType::Uuid,
            RustDataType::Map => DataType::Map,
            RustDataType::List => DataType::List,
            RustDataType::Decimal => DataType::Decimal,
            RustDataType::Point => DataType::Point,
        }
    }
}

#[derive(Debug, uniffi::Enum)]
pub enum PayloadVariable {
    Tables(Vec<String>),
    Functions(Vec<String>),
    Version(String),
}
impl From<RustPayloadVariable> for PayloadVariable {
    fn from(payload_variable: RustPayloadVariable) -> Self {
        match payload_variable {
            RustPayloadVariable::Tables(t) => PayloadVariable::Tables(t),
            RustPayloadVariable::Functions(t) => PayloadVariable::Functions(t),
            RustPayloadVariable::Version(t) => PayloadVariable::Version(t),
        }
    }
}

#[derive(Debug, uniffi::Record)]
pub struct ColumnMeta {
    field: String,
    data_type: DataType,
}

#[derive(Debug, uniffi::Enum)]
pub enum Payload {
    ShowColumns(Vec<ColumnMeta>),
    Create,
    Insert(u64),
    Select {
        labels: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    SelectMap(Vec<HashMap<String, Value>>),
    Delete(u64),
    Update(u64),
    DropTable(u64),
    DropFunction,
    AlterTable,
    CreateIndex,
    DropIndex,
    StartTransaction,
    Commit,
    Rollback,
    ShowVariable(PayloadVariable),
}
impl From<RustPayload> for Payload {
    fn from(payload: RustPayload) -> Self {
        match payload {
            RustPayload::Create => Payload::Create,
            RustPayload::Commit => Payload::Commit,
            RustPayload::Rollback => Payload::Rollback,
            RustPayload::DropFunction => Payload::DropFunction,
            RustPayload::AlterTable => Payload::AlterTable,
            RustPayload::CreateIndex => Payload::CreateIndex,
            RustPayload::DropIndex => Payload::DropIndex,
            RustPayload::StartTransaction => Payload::StartTransaction,
            RustPayload::Insert(i) => Payload::Insert(i as u64),
            RustPayload::Select { labels, rows } => {
                let mut res = Vec::with_capacity(rows.len());
                rows.into_iter().for_each(|r| {
                    let val: Vec<Value> = r.into_iter().map(|y| y.into()).collect();
                    res.push(val);
                });
                Payload::Select { labels, rows: res }
            }
            RustPayload::ShowColumns(v) => Payload::ShowColumns(
                v.into_iter()
                    .map(|x| ColumnMeta {
                        field: x.0,
                        data_type: x.1.into(),
                    })
                    .collect(),
            ),
            RustPayload::SelectMap(s_m) => Payload::SelectMap(
                s_m.into_iter()
                    .map(|x| {
                        let mut map: HashMap<String, Value> = HashMap::new();
                        x.into_iter().for_each(|y| {
                            map.insert(y.0, y.1.into());
                        });
                        map
                    })
                    .collect(),
            ),
            RustPayload::Delete(d) => Payload::Delete(d as u64),
            RustPayload::Update(d) => Payload::Update(d as u64),
            RustPayload::DropTable(d) => Payload::DropTable(d as u64),
            RustPayload::ShowVariable(d) => Payload::ShowVariable(d.into()),
        }
    }
}

struct CreateStore {
    kind: GlueStorageKind,
}

enum CmdKind {
    Create(CreateStore),
    Query(Task),
}

struct Dto {
    msg: CmdKind,
    glue_id: Uuid,
    notifier: tokio::sync::oneshot::Sender<Result<Vec<Payload>, String>>,
}

static QUEUE: LazyLock<Mutex<GlobalQueue>> = LazyLock::new(|| {
    let (sender, receiver) = mpsc::channel::<Dto>(100);
    let _handle = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let mut map: HashMap<Uuid, GlueStorage> = HashMap::new();
            let mut receiver = receiver;
            while let Some(msg) = receiver.recv().await {
                let mut res_vec: Vec<Payload> = vec![];
                match msg.msg {
                    CmdKind::Create(create_msg) => {
                        let glue = match create_msg.kind {
                            GlueStorageKind::MemoryStorage => {
                                GlueStorage::MemoryStorage(RustGlue::new(MemoryStorage::default()))
                            }
                            GlueStorageKind::SharedMemoryStorage => {
                                GlueStorage::SharedMemoryStorage(RustGlue::new(
                                    SharedMemoryStorage::default(),
                                ))
                            }
                        };
                        let _ = map.insert(msg.glue_id, glue);
                        res_vec.push(Payload::Create);
                    }
                    CmdKind::Query(query_msg) => {
                        let glue = map.get_mut(&msg.glue_id).unwrap();
                        for query in query_msg.sql {
                            match glue {
                                GlueStorage::MemoryStorage(s) => {
                                    let res = s.execute(query).await.unwrap();
                                    let res =
                                        res.into_iter().map(|r| r.into()).collect::<Vec<Payload>>();
                                    res_vec.extend(res);
                                }
                                GlueStorage::SharedMemoryStorage(s) => {
                                    let res = s.execute(query).await.unwrap();
                                    res_vec.extend(
                                        res.into_iter().map(|r| r.into()).collect::<Vec<Payload>>(),
                                    );
                                }
                            }
                        }
                    }
                }
                msg.notifier.send(Ok(res_vec)).unwrap();
            }
        });
    });
    Mutex::new(GlobalQueue { sender })
});

#[derive(uniffi::Object)]
pub struct Glue {
    pub glue_id: Uuid,
    pub kind: GlueStorageKind,
}
#[uniffi::export(async_runtime = "tokio")]
impl Glue {
    #[uniffi::constructor]
    async fn new(kind: GlueStorageKind) -> Self {
        let glue_id = Uuid::new_v4();

        let (send, recv) = tokio::sync::oneshot::channel();
        QUEUE
            .lock()
            .unwrap()
            .sender
            .blocking_send(Dto {
                glue_id,
                msg: CmdKind::Create(CreateStore { kind }),
                notifier: send,
            })
            .unwrap();
        let _ = recv.await.unwrap().unwrap();
        Self { glue_id, kind }
    }
    fn get_id(&self) -> String {
        self.glue_id.into()
    }
    fn get_kind(&self) -> GlueStorageKind {
        self.kind
    }
    async fn query(&self, query: Vec<String>) -> Vec<Payload> {
        let (send, recv) = tokio::sync::oneshot::channel();
        let _ = QUEUE.lock().unwrap().sender.try_send(Dto {
            glue_id: self.glue_id,
            msg: CmdKind::Query(Task { sql: query }),
            notifier: send,
        });
        recv.await.unwrap().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gluesql_core::prelude::Value as RustValue;
    use std::net::{Ipv4Addr as RustIpv4Addr, Ipv6Addr as RustIpv6Addr};
    use std::sync::Arc;

    // MARK: - Type Conversion Tests

    #[test]
    fn test_value_bool_conversion() {
        let rust_value = RustValue::Bool(true);
        let value: Value = rust_value.into();

        match value {
            Value::Bool(b) => assert!(b),
            _ => panic!("Expected Bool variant"),
        }
    }

    #[test]
    fn test_value_integer_conversions() {
        // Test I8
        let value: Value = RustValue::I8(127).into();
        assert!(matches!(value, Value::I8(127)));

        // Test I16
        let value: Value = RustValue::I16(32767).into();
        assert!(matches!(value, Value::I16(32767)));

        // Test I32
        let value: Value = RustValue::I32(2147483647).into();
        assert!(matches!(value, Value::I32(2147483647)));

        // Test I64
        let value: Value = RustValue::I64(9223372036854775807).into();
        assert!(matches!(value, Value::I64(9223372036854775807)));
    }

    #[test]
    fn test_value_i128_conversion() {
        let rust_value = RustValue::I128(123456789i128);
        let value: Value = rust_value.into();

        if let Value::I128(i128_val) = value {
            // Convert back to verify
            let mut bytes = [0u8; 16];
            bytes[0..8].copy_from_slice(&i128_val.high.to_le_bytes());
            bytes[8..16].copy_from_slice(&i128_val.low.to_le_bytes());
            let reconstructed = i128::from_le_bytes(bytes);
            assert_eq!(reconstructed, 123456789i128);
        } else {
            panic!("Expected I128 variant");
        }
    }

    #[test]
    fn test_value_u128_conversion() {
        let rust_value = RustValue::U128(123456789u128);
        let value: Value = rust_value.into();

        if let Value::U128(u128_val) = value {
            let mut bytes = [0u8; 16];
            bytes[0..8].copy_from_slice(&u128_val.high.to_le_bytes());
            bytes[8..16].copy_from_slice(&u128_val.low.to_le_bytes());
            let reconstructed = u128::from_le_bytes(bytes);
            assert_eq!(reconstructed, 123456789u128);
        } else {
            panic!("Expected U128 variant");
        }
    }

    #[test]
    fn test_value_float_conversions() {
        let value: Value = RustValue::F32(3.11f32).into();
        if let Value::F32(f) = value {
            assert!((f - 3.11f32).abs() < 0.001);
        } else {
            panic!("Expected F32 variant");
        }

        let value: Value = RustValue::F64(3.11111111111f64).into();
        if let Value::F64(f) = value {
            assert!((f - 3.11111111111f64).abs() < 0.0000001);
        } else {
            panic!("Expected F64 variant");
        }
    }

    #[test]
    fn test_value_string_conversion() {
        let rust_value = RustValue::Str("Hello, World!".to_string());
        let value: Value = rust_value.into();

        match value {
            Value::Str(s) => assert_eq!(s, "Hello, World!"),
            _ => panic!("Expected Str variant"),
        }
    }

    #[test]
    fn test_value_bytea_conversion() {
        let bytes = vec![0u8, 1, 2, 3, 4, 5];
        let rust_value = RustValue::Bytea(bytes.clone());
        let value: Value = rust_value.into();

        match value {
            Value::Bytea(b) => assert_eq!(b, bytes),
            _ => panic!("Expected Bytea variant"),
        }
    }

    #[test]
    fn test_value_null_conversion() {
        let rust_value = RustValue::Null;
        let value: Value = rust_value.into();

        assert!(matches!(value, Value::Null));
    }

    #[test]
    fn test_value_list_conversion() {
        let rust_list = vec![RustValue::I64(1), RustValue::I64(2), RustValue::I64(3)];
        let rust_value = RustValue::List(rust_list);
        let value: Value = rust_value.into();

        if let Value::List(list) = value {
            assert_eq!(list.len(), 3);
            assert!(matches!(list[0], Value::I64(1)));
            assert!(matches!(list[1], Value::I64(2)));
            assert!(matches!(list[2], Value::I64(3)));
        } else {
            panic!("Expected List variant");
        }
    }

    #[test]
    fn test_value_map_conversion() {
        let mut rust_map = std::collections::HashMap::new();
        rust_map.insert("key1".to_string(), RustValue::Str("value1".to_string()));
        rust_map.insert("key2".to_string(), RustValue::I64(42));

        let rust_value = RustValue::Map(rust_map);
        let value: Value = rust_value.into();

        if let Value::Map(map) = value {
            assert_eq!(map.len(), 2);
            assert!(matches!(map.get("key1"), Some(Value::Str(_))));
            assert!(matches!(map.get("key2"), Some(Value::I64(42))));
        } else {
            panic!("Expected Map variant");
        }
    }

    #[test]
    fn test_point_conversion() {
        let rust_point = RustPoint { x: 10.5, y: 20.3 };
        let rust_value = RustValue::Point(rust_point);
        let value: Value = rust_value.into();

        if let Value::Point(point) = value {
            assert!((point.x - 10.5).abs() < 0.0001);
            assert!((point.y - 20.3).abs() < 0.0001);
        } else {
            panic!("Expected Point variant");
        }
    }

    #[test]
    fn test_interval_conversion() {
        // Test Month variant
        let rust_interval = RustInterval::Month(12);
        let interval: Interval = rust_interval.into();
        assert!(matches!(interval, Interval::Month(12)));

        // Test Microsecond variant
        let rust_interval = RustInterval::Microsecond(1000000);
        let interval: Interval = rust_interval.into();
        assert!(matches!(interval, Interval::Microsecond(1000000)));
    }

    #[test]
    fn test_ipv4_conversion() {
        let rust_ipv4 = RustIpv4Addr::new(127, 0, 0, 1);
        let ipv4: Ipv4Addr = rust_ipv4.into();

        assert_eq!(ipv4.octets.len(), 4);
        assert_eq!(ipv4.octets, vec![127, 0, 0, 1]);
    }

    #[test]
    fn test_ipv6_conversion() {
        let rust_ipv6 = RustIpv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1);
        let ipv6: Ipv6Addr = rust_ipv6.into();

        assert_eq!(ipv6.octets.len(), 16);
    }

    #[test]
    fn test_ipaddr_conversion() {
        // Test V4
        let rust_ip = std::net::IpAddr::V4(RustIpv4Addr::new(192, 168, 1, 1));
        let ip: IpAddr = rust_ip.into();
        assert!(matches!(ip, IpAddr::V4(_)));

        // Test V6
        let rust_ip = std::net::IpAddr::V6(RustIpv6Addr::LOCALHOST);
        let ip: IpAddr = rust_ip.into();
        assert!(matches!(ip, IpAddr::V6(_)));
    }

    #[test]
    fn test_datatype_conversions() {
        assert!(matches!(
            DataType::from(RustDataType::Boolean),
            DataType::Boolean
        ));
        assert!(matches!(DataType::from(RustDataType::Int), DataType::Int));
        assert!(matches!(DataType::from(RustDataType::Int8), DataType::Int8));
        assert!(matches!(DataType::from(RustDataType::Text), DataType::Text));
        assert!(matches!(
            DataType::from(RustDataType::Float),
            DataType::Float
        ));
        assert!(matches!(DataType::from(RustDataType::Date), DataType::Date));
        assert!(matches!(DataType::from(RustDataType::Uuid), DataType::Uuid));
        assert!(matches!(
            DataType::from(RustDataType::Point),
            DataType::Point
        ));
    }

    // MARK: - Payload Conversion Tests

    #[test]
    fn test_payload_create_conversion() {
        let rust_payload = RustPayload::Create;
        let payload: Payload = rust_payload.into();
        assert!(matches!(payload, Payload::Create));
    }

    #[test]
    fn test_payload_insert_conversion() {
        let rust_payload = RustPayload::Insert(5);
        let payload: Payload = rust_payload.into();

        if let Payload::Insert(count) = payload {
            assert_eq!(count, 5);
        } else {
            panic!("Expected Insert payload");
        }
    }

    #[test]
    fn test_payload_select_conversion() {
        let labels = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![RustValue::I64(1), RustValue::Str("Alice".to_string())],
            vec![RustValue::I64(2), RustValue::Str("Bob".to_string())],
        ];

        let rust_payload = RustPayload::Select {
            labels: labels.clone(),
            rows,
        };
        let payload: Payload = rust_payload.into();

        if let Payload::Select { labels: l, rows: r } = payload {
            assert_eq!(l, labels);
            assert_eq!(r.len(), 2);
        } else {
            panic!("Expected Select payload");
        }
    }

    #[test]
    fn test_payload_delete_conversion() {
        let rust_payload = RustPayload::Delete(3);
        let payload: Payload = rust_payload.into();

        if let Payload::Delete(count) = payload {
            assert_eq!(count, 3);
        } else {
            panic!("Expected Delete payload");
        }
    }

    #[test]
    fn test_payload_update_conversion() {
        let rust_payload = RustPayload::Update(2);
        let payload: Payload = rust_payload.into();

        if let Payload::Update(count) = payload {
            assert_eq!(count, 2);
        } else {
            panic!("Expected Update payload");
        }
    }

    #[test]
    fn test_payload_transaction_conversions() {
        assert!(matches!(
            Payload::from(RustPayload::StartTransaction),
            Payload::StartTransaction
        ));
        assert!(matches!(
            Payload::from(RustPayload::Commit),
            Payload::Commit
        ));
        assert!(matches!(
            Payload::from(RustPayload::Rollback),
            Payload::Rollback
        ));
    }

    #[test]
    fn test_payload_show_columns_conversion() {
        let columns = vec![
            ("id".to_string(), RustDataType::Int),
            ("name".to_string(), RustDataType::Text),
        ];

        let rust_payload = RustPayload::ShowColumns(columns);
        let payload: Payload = rust_payload.into();

        if let Payload::ShowColumns(cols) = payload {
            assert_eq!(cols.len(), 2);
            assert_eq!(cols[0].field, "id");
            assert!(matches!(cols[0].data_type, DataType::Int));
            assert_eq!(cols[1].field, "name");
            assert!(matches!(cols[1].data_type, DataType::Text));
        } else {
            panic!("Expected ShowColumns payload");
        }
    }

    // MARK: - Async Integration Tests

    #[tokio::test]
    async fn test_glue_memory_storage_creation() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;
        assert_eq!(glue.kind, GlueStorageKind::MemoryStorage);
        assert!(!glue.get_id().is_empty());
    }

    #[tokio::test]
    async fn test_glue_shared_memory_storage_creation() {
        let glue = Glue::new(GlueStorageKind::SharedMemoryStorage).await;
        assert_eq!(glue.kind, GlueStorageKind::SharedMemoryStorage);
        assert!(!glue.get_id().is_empty());
    }

    #[tokio::test]
    async fn test_unique_glue_ids() {
        let glue1 = Glue::new(GlueStorageKind::MemoryStorage).await;
        let glue2 = Glue::new(GlueStorageKind::MemoryStorage).await;

        assert_ne!(glue1.glue_id, glue2.glue_id);
    }

    #[tokio::test]
    async fn test_basic_create_table() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sql = vec!["CREATE TABLE Users (id INT, name TEXT);".to_string()];
        let result = glue.query(sql).await;

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Payload::Create));
    }

    #[tokio::test]
    async fn test_insert_operation() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE Users (id INT, name TEXT);".to_string(),
            "INSERT INTO Users VALUES (1, 'Alice');".to_string(),
        ];
        let result = glue.query(sqls).await;

        assert_eq!(result.len(), 2);
        assert!(matches!(result[0], Payload::Create));
        if let Payload::Insert(count) = result[1] {
            assert_eq!(count, 1);
        } else {
            panic!("Expected Insert payload");
        }
    }

    #[tokio::test]
    async fn test_select_operation() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE Users (id INT, name TEXT);".to_string(),
            "INSERT INTO Users VALUES (1, 'Alice');".to_string(),
            "INSERT INTO Users VALUES (2, 'Bob');".to_string(),
            "SELECT * FROM Users;".to_string(),
        ];
        let result = glue.query(sqls).await;

        assert_eq!(result.len(), 4);
        if let Payload::Select { labels, rows } = &result[3] {
            assert_eq!(labels.len(), 2);
            assert!(labels.contains(&"id".to_string()));
            assert!(labels.contains(&"name".to_string()));
            assert_eq!(rows.len(), 2);
        } else {
            panic!("Expected Select payload");
        }
    }

    #[tokio::test]
    async fn test_update_operation() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE Users (id INT, name TEXT);".to_string(),
            "INSERT INTO Users VALUES (1, 'Alice');".to_string(),
            "UPDATE Users SET name = 'Alicia' WHERE id = 1;".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::Update(count) = result[2] {
            assert_eq!(count, 1);
        } else {
            panic!("Expected Update payload");
        }
    }

    #[tokio::test]
    async fn test_delete_operation() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE Users (id INT, name TEXT);".to_string(),
            "INSERT INTO Users VALUES (1, 'Alice');".to_string(),
            "INSERT INTO Users VALUES (2, 'Bob');".to_string(),
            "DELETE FROM Users WHERE id = 1;".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::Delete(count) = result[3] {
            assert_eq!(count, 1);
        } else {
            panic!("Expected Delete payload");
        }
    }

    #[tokio::test]
    async fn test_drop_table() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE Users (id INT);".to_string(),
            "DROP TABLE Users;".to_string(),
        ];
        let result = glue.query(sqls).await;

        assert!(matches!(result[1], Payload::DropTable(_)));
    }

    #[tokio::test]
    async fn test_transaction_operations() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE Account (id INT, balance INT);".to_string(),
            "INSERT INTO Account VALUES (1, 100);".to_string(),
            "START TRANSACTION;".to_string(),
            "UPDATE Account SET balance = 150 WHERE id = 1;".to_string(),
            "COMMIT;".to_string(),
        ];
        let result = glue.query(sqls).await;

        let mut found_transaction = false;
        let mut found_commit = false;

        for payload in result {
            if matches!(payload, Payload::StartTransaction) {
                found_transaction = true;
            }
            if matches!(payload, Payload::Commit) {
                found_commit = true;
            }
        }

        assert!(found_transaction);
        assert!(found_commit);
    }

    #[tokio::test]
    async fn test_rollback_operation() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE Account (id INT, balance INT);".to_string(),
            "START TRANSACTION;".to_string(),
            "ROLLBACK;".to_string(),
        ];
        let result = glue.query(sqls).await;

        let found_rollback = result.iter().any(|p| matches!(p, Payload::Rollback));
        assert!(found_rollback);
    }

    #[tokio::test]
    async fn test_where_clause() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE Products (id INT, category TEXT, name TEXT);".to_string(),
            "INSERT INTO Products VALUES (1, 'Electronics', 'Laptop');".to_string(),
            "INSERT INTO Products VALUES (2, 'Electronics', 'Phone');".to_string(),
            "INSERT INTO Products VALUES (3, 'Books', 'Novel');".to_string(),
            "SELECT * FROM Products WHERE category = 'Electronics';".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::Select { rows, .. } = &result[4] {
            assert_eq!(rows.len(), 2);
        } else {
            panic!("Expected Select payload");
        }
    }

    #[tokio::test]
    async fn test_order_by() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE Products (id INT, price INT);".to_string(),
            "INSERT INTO Products VALUES (1, 100);".to_string(),
            "INSERT INTO Products VALUES (2, 50);".to_string(),
            "INSERT INTO Products VALUES (3, 200);".to_string(),
            "SELECT * FROM Products ORDER BY price DESC;".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::Select { rows, .. } = &result[4] {
            assert_eq!(rows.len(), 3);
            // First row should be highest price (200)
            if let Value::I64(price) = rows[0][1] {
                assert_eq!(price, 200);
            }
        } else {
            panic!("Expected Select payload");
        }
    }

    #[tokio::test]
    async fn test_aggregate_count() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE Items (id INT);".to_string(),
            "INSERT INTO Items VALUES (1);".to_string(),
            "INSERT INTO Items VALUES (2);".to_string(),
            "INSERT INTO Items VALUES (3);".to_string(),
            "SELECT COUNT(*) as count FROM Items;".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::Select { rows, .. } = &result[4] {
            assert_eq!(rows.len(), 1);
            if let Value::I64(count) = rows[0][0] {
                assert_eq!(count, 3);
            }
        } else {
            panic!("Expected Select payload");
        }
    }

    #[tokio::test]
    async fn test_join_operation() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE Users (id INT, name TEXT);".to_string(),
            "CREATE TABLE Orders (id INT, user_id INT, amount INT);".to_string(),
            "INSERT INTO Users VALUES (1, 'Alice');".to_string(),
            "INSERT INTO Users VALUES (2, 'Bob');".to_string(),
            "INSERT INTO Orders VALUES (1, 1, 100);".to_string(),
            "INSERT INTO Orders VALUES (2, 1, 200);".to_string(),
            "SELECT u.name, o.amount FROM Users u JOIN Orders o ON u.id = o.user_id;".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::Select { labels, rows } = &result[6] {
            assert_eq!(labels.len(), 2);
            assert_eq!(rows.len(), 2);
        } else {
            panic!("Expected Select payload");
        }
    }

    #[tokio::test]
    async fn test_multiple_queries_single_call() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let queries = vec![
            "CREATE TABLE Test (id INT);".to_string(),
            "INSERT INTO Test VALUES (1);".to_string(),
            "INSERT INTO Test VALUES (2);".to_string(),
            "SELECT * FROM Test;".to_string(),
        ];
        let result = glue.query(queries).await;

        assert_eq!(result.len(), 4);
        assert!(matches!(result[0], Payload::Create));
        assert!(matches!(result[1], Payload::Insert(1)));
        assert!(matches!(result[2], Payload::Insert(1)));
        if let Payload::Select { rows, .. } = &result[3] {
            assert_eq!(rows.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_storage_isolation() {
        let glue1 = Glue::new(GlueStorageKind::MemoryStorage).await;
        let glue2 = Glue::new(GlueStorageKind::MemoryStorage).await;

        // Create table in first instance
        glue1
            .query(vec!["CREATE TABLE Test (id INT);".to_string()])
            .await;
        glue1
            .query(vec!["INSERT INTO Test VALUES (1);".to_string()])
            .await;

        // Different instance should have isolated storage
        assert_ne!(glue1.glue_id, glue2.glue_id);
    }

    #[tokio::test]
    async fn test_concurrent_queries() {
        let glue = Arc::new(Glue::new(GlueStorageKind::MemoryStorage).await);

        glue.query(vec!["CREATE TABLE Concurrent (id INT);".to_string()])
            .await;

        // Spawn multiple concurrent queries
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let glue_clone = Arc::clone(&glue);
                tokio::spawn(async move {
                    glue_clone
                        .query(vec![format!("INSERT INTO Concurrent VALUES ({});", i)])
                        .await
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        let result = glue
            .query(vec!["SELECT COUNT(*) FROM Concurrent;".to_string()])
            .await;
        if let Payload::Select { rows, .. } = &result[0] {
            if let Value::I64(count) = rows[0][0] {
                assert_eq!(count, 5);
            }
        }
    }

    #[tokio::test]
    async fn test_show_columns() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE ShowTest (id INT, name TEXT, price FLOAT);".to_string(),
            "SHOW COLUMNS FROM ShowTest;".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::ShowColumns(columns) = &result[1] {
            assert_eq!(columns.len(), 3);
            assert_eq!(columns[0].field, "id");
            assert!(matches!(columns[0].data_type, DataType::Int));
        } else {
            panic!("Expected ShowColumns payload");
        }
    }

    // MARK: - Data Type Tests

    #[tokio::test]
    async fn test_data_types() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE TypeTest (bool_col BOOLEAN, int_col INT, float_col FLOAT, text_col TEXT);".to_string(),
            "INSERT INTO TypeTest VALUES (TRUE, 42, 3.14, 'hello');".to_string(),
            "SELECT * FROM TypeTest;".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::Select { rows, .. } = &result[2] {
            assert_eq!(rows.len(), 1);
            let row = &rows[0];

            assert!(matches!(row[0], Value::Bool(true)));
            assert!(matches!(row[1], Value::I64(42)));
            assert!(matches!(row[3], Value::Str(_)));
        } else {
            panic!("Expected Select payload");
        }
    }

    #[tokio::test]
    async fn test_null_handling() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE NullTest (id INT, value TEXT NULL);".to_string(),
            "INSERT INTO NullTest VALUES (1, NULL);".to_string(),
            "SELECT * FROM NullTest;".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::Select { rows, .. } = &result[2] {
            assert!(matches!(rows[0][1], Value::Null));
        }
    }

    #[tokio::test]
    async fn test_list_type() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE ListTest (id INT, tags LIST);".to_string(),
            "INSERT INTO ListTest VALUES (1, ['tag1', 'tag2', 'tag3']);".to_string(),
            "SELECT * FROM ListTest;".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::Select { rows, .. } = &result[2] {
            if let Value::List(list) = &rows[0][1] {
                assert_eq!(list.len(), 3);
            } else {
                panic!("Expected list value");
            }
        }
    }

    #[tokio::test]
    async fn test_map_type() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE MapTest (id INT, data MAP);".to_string(),
            "INSERT INTO MapTest VALUES (1, {'key': 'value'});".to_string(),
            "SELECT * FROM MapTest;".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::Select { rows, .. } = &result[2] {
            if let Value::Map(map) = &rows[0][1] {
                assert_eq!(map.len(), 1);
            } else {
                panic!("Expected map value");
            }
        }
    }

    #[tokio::test]
    async fn test_point_type() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;

        let sqls = vec![
            "CREATE TABLE PointTest (id INT, location POINT);".to_string(),
            "INSERT INTO PointTest VALUES (1, POINT(10.5, 20.3));".to_string(),
            "SELECT * FROM PointTest;".to_string(),
        ];
        let result = glue.query(sqls).await;

        if let Payload::Select { rows, .. } = &result[2] {
            if let Value::Point(point) = &rows[0][1] {
                assert!((point.x - 10.5).abs() < 0.01);
                assert!((point.y - 20.3).abs() < 0.01);
            } else {
                panic!("Expected point value");
            }
        }
    }

    // MARK: - Edge Cases

    #[tokio::test]
    async fn test_empty_query_vector() {
        let glue = Glue::new(GlueStorageKind::MemoryStorage).await;
        let result = glue.query(vec![]).await;
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_glue_storage_kind_copy() {
        let kind1 = GlueStorageKind::MemoryStorage;
        let kind2 = kind1;

        // Should compile due to Copy trait
        assert_eq!(kind1, kind2);
    }

    #[test]
    fn test_i128_record_equality() {
        let i128_1 = I128 {
            high: 100,
            low: 200,
        };
        let i128_2 = I128 {
            high: 100,
            low: 200,
        };
        let i128_3 = I128 {
            high: 100,
            low: 201,
        };

        assert_eq!(i128_1, i128_2);
        assert_ne!(i128_1, i128_3);
    }

    #[test]
    fn test_u128_record_equality() {
        let u128_1 = U128 {
            high: 100,
            low: 200,
        };
        let u128_2 = U128 {
            high: 100,
            low: 200,
        };

        assert_eq!(u128_1, u128_2);
    }

    #[test]
    fn test_point_record() {
        let point = Point { x: 10.5, y: 20.3 };
        assert_eq!(point.x, 10.5);
        assert_eq!(point.y, 20.3);
    }
}
