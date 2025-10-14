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
    sync::{Arc, LazyLock},
    thread,
};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

uniffi::setup_scaffolding!("gluesql");

enum GlueStorage {
    MemoryStorage(RustGlue<MemoryStorage>),
    SharedMemoryStorage(RustGlue<SharedMemoryStorage>),
}

#[derive(uniffi::Enum, Clone, Copy)]
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
    I128(String),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(String),
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
                // let raw_vec = v.to_le_bytes();
                // let high = u64::from_le_bytes(raw_vec[0..8].try_into().unwrap());
                // let low = u64::from_le_bytes(raw_vec[8..16].try_into().unwrap());
                // Value::I128(I128 { high, low })
                Value::I128(v.to_string())
            }
            RustValue::U8(v) => Value::U8(v),
            RustValue::U16(v) => Value::U16(v),
            RustValue::U32(v) => Value::U32(v),
            RustValue::U64(v) => Value::U64(v),
            RustValue::U128(v) => {
                // let raw_vec = v.to_le_bytes();
                // let high = u64::from_le_bytes(raw_vec[0..8].try_into().unwrap());
                // let low = u64::from_le_bytes(raw_vec[8..16].try_into().unwrap());
                // Value::U128(U128 { high, low })
                Value::U128(v.to_string())
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
            RustValue::Uuid(v) => Value::Uuid(Uuid::from_u128(v).as_hyphenated().to_string()),
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

#[derive(uniffi::Object)]
pub struct Glue(Mutex<GlueStorage>);
#[uniffi::export(async_runtime = "tokio")]
impl Glue {
    #[uniffi::constructor]
    async fn new(kind: GlueStorageKind) -> Arc<Self> {
        let glue = match kind {
            GlueStorageKind::MemoryStorage => {
                GlueStorage::MemoryStorage(RustGlue::new(MemoryStorage::default()))
            }
            GlueStorageKind::SharedMemoryStorage => {
                GlueStorage::SharedMemoryStorage(RustGlue::new(SharedMemoryStorage::default()))
            }
        };
        Arc::new(Glue(Mutex::new(glue)))
    }
    async fn get_kind(&self) -> GlueStorageKind {
        match *self.0.lock().await {
            GlueStorage::MemoryStorage(_) => GlueStorageKind::MemoryStorage,
            GlueStorage::SharedMemoryStorage(_) => GlueStorageKind::SharedMemoryStorage,
        }
    }
    async fn query(&self, query: Vec<String>) -> Vec<Payload> {
        let mut res_vec = vec![];
        let mut storage_guard = self.0.lock().await;
        let storage: &mut GlueStorage = &mut storage_guard;
        for query in query {
            match storage {
                GlueStorage::MemoryStorage(s) => {
                    let res = s.execute(query).await.unwrap();
                    let res = res.into_iter().map(|r| r.into()).collect::<Vec<Payload>>();
                    res_vec.extend(res);
                }
                GlueStorage::SharedMemoryStorage(s) => {
                    let res = s.execute(query).await.unwrap();
                    res_vec.extend(res.into_iter().map(|r| r.into()).collect::<Vec<Payload>>());
                }
            }
        }
        res_vec
    }
    async fn add_dummy_data(&self) -> Vec<Payload> {
        let sqls = vec![
            "DROP TABLE IF EXISTS OrderDetails;
DROP TABLE IF EXISTS Orders;
DROP TABLE IF EXISTS Products;
DROP TABLE IF EXISTS Customers;".into(),
            "CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    FirstName TEXT,
    LastName TEXT,
    Email TEXT UNIQUE,
    RegistrationDate DATE
);".into(),
            "CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName TEXT,
    Category TEXT,
    Price DECIMAL
);".into(),
            "CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATE,
    TotalAmount DECIMAL,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);".into(),
            "CREATE TABLE OrderDetails (
    OrderDetailID INT PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    Quantity INT,
    PricePerUnit DECIMAL,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);".into(),
            "INSERT INTO Customers (CustomerID, FirstName, LastName, Email, RegistrationDate) VALUES
(1, 'John', 'Smith', 'john.smith@email.com', '2022-01-15'),
(2, 'Jane', 'Doe', 'jane.doe@email.com', '2022-02-20'),
(3, 'Peter', 'Jones', 'peter.jones@email.com', '2022-03-10'),
(4, 'Maria', 'Garcia', 'maria.garcia@email.com', '2023-05-01');
".into(),
            "INSERT INTO Products (ProductID, ProductName, Category, Price) VALUES
(101, 'Laptop', 'Electronics', 1200.00),
(102, 'Smartphone', 'Electronics', 800.00),
(103, 'Coffee Maker', 'Home Goods', 75.50),
(104, 'Desk Chair', 'Furniture', 250.00),
(105, 'SQL for Beginners', 'Books', 45.99);".into(),
            "INSERT INTO Orders (OrderID, CustomerID, OrderDate, TotalAmount) VALUES
(1001, 1, '2023-04-10', 1245.99),
(1002, 2, '2023-04-12', 800.00),
(1003, 1, '2023-05-21', 75.50),
(1004, 3, '2023-06-01', 295.99);".into(),
            "INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity, PricePerUnit) VALUES
(1, 1001, 101, 1, 1200.00),
(2, 1001, 105, 1, 45.99),
(3, 1002, 102, 1, 800.00),
(4, 1003, 103, 1, 75.50),
(5, 1004, 104, 1, 250.00),
(6, 1004, 105, 1, 45.99);

".into(),
        ];
        self.query(sqls).await
    }
}
