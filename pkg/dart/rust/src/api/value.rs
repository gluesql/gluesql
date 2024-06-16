use flutter_rust_bridge::frb;
pub use gluesql_core::{
    chrono::{NaiveDate, NaiveDateTime, NaiveTime},
    data::{Interval, Point, Value},
};
pub use rust_decimal::Decimal;
use std::{collections::HashMap, net::IpAddr};

#[frb(non_opaque)]
pub enum DartValue {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    F32(f32),
    F64(f64),
    Decimal(Decimal),
    Str(String),
    Bytea(Vec<u8>),
    Inet(IpAddr),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Time(NaiveTime),
    Interval(Interval),
    Uuid(u128),
    Map(HashMap<String, Value>),
    List(Vec<Value>),
    Point(Point),
    NullData, // TODO: rename
}

impl From<Value> for DartValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Bool(value) => DartValue::Bool(value),
            Value::I8(value) => DartValue::I8(value),
            Value::I16(value) => DartValue::I16(value),
            Value::I32(value) => DartValue::I32(value),
            Value::I64(value) => DartValue::I64(value),
            Value::I128(value) => DartValue::I128(value),
            Value::U8(value) => DartValue::U8(value),
            Value::U16(value) => DartValue::U16(value),
            Value::U32(value) => DartValue::U32(value),
            Value::U64(value) => DartValue::U64(value),
            Value::U128(value) => DartValue::U128(value),
            Value::F32(value) => DartValue::F32(value),
            Value::F64(value) => DartValue::F64(value),
            Value::Decimal(value) => DartValue::Decimal(value),
            Value::Str(value) => DartValue::Str(value),
            Value::Bytea(value) => DartValue::Bytea(value),
            Value::Inet(value) => DartValue::Inet(value),
            Value::Date(value) => DartValue::Date(value),
            Value::Timestamp(value) => DartValue::Timestamp(value),
            Value::Time(value) => DartValue::Time(value),
            Value::Interval(value) => DartValue::Interval(value),
            Value::Uuid(value) => DartValue::Uuid(value),
            Value::Map(value) => DartValue::Map(value),
            Value::List(value) => DartValue::List(value),
            Value::Point(value) => DartValue::Point(value),
            Value::Null => DartValue::NullData,
        }
    }
}

// impl IntoIntoDart<DartValue> for DartValue {
//     fn into_into_dart(self) -> DartValue {
//         self
//     }
// }

// impl flutter_rust_bridge::IntoIntoDart<FrbWrapper<NaiveDateTime>> for NaiveDateTime {
//     fn into_into_dart(self) -> FrbWrapper<NaiveDateTime> {
//         self.into()
//     }
// }

// impl From<FrbWrapper<NaiveDateTime>> for allo_isolate::ffi::DartCObject {
//     fn from(item: FrbWrapper<NaiveDateTime>) -> Self {
//         // Your conversion code here
//     }
// }
