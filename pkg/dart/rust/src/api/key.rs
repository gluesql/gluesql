use flutter_rust_bridge::frb;
pub use gluesql_core::data::{Interval, Key};
pub use {
    gluesql_core::chrono::{NaiveDate, NaiveDateTime, NaiveTime},
    ordered_float::OrderedFloat,
    rust_decimal::Decimal,
    std::net::IpAddr,
};

// #[frb(mirror(Key), non_opaque)]
// enum _Key {
//     I8(i8),
//     I16(i16),
//     I32(i32),
//     I64(i64),
//     I128(i128),
//     U8(u8),
//     U16(u16),
//     U32(u32),
//     U64(u64),
//     U128(u128),
//     F32(OrderedFloat<f32>),
//     F64(OrderedFloat<f64>),
//     Decimal(Decimal),
//     Bool(bool),
//     Str(String),
//     Bytea(Vec<u8>),
//     Date(NaiveDate),
//     Timestamp(NaiveDateTime),
//     Time(NaiveTime),
//     Interval(Interval),
//     Uuid(u128),
//     Inet(IpAddr),
//     None,
// }
