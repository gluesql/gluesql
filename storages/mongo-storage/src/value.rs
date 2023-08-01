use std::{
    collections::HashMap,
    fmt::{self, Display},
    io::{Bytes, Read},
    str::FromStr,
};

use bson::Timestamp;
use chrono::{NaiveDateTime, TimeZone};
use gluesql_core::{
    chrono::{NaiveDate, Utc},
    data::Point,
    prelude::{DataType, Key},
    store::DataRow,
};
use mongodb::bson::{
    self, bson, de, doc, spec::ElementType, to_bson, Binary, Bson, DateTime, Decimal128, Document,
    Uuid,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use strum_macros::{Display, EnumString, IntoStaticStr};

use crate::error::ResultExt;

use {gluesql_core::data::Value, gluesql_core::prelude::Result};

pub trait IntoValue {
    fn into_value(self) -> Value;
    fn into_value2(self, data_type: &DataType) -> Value;
}

impl IntoValue for Bson {
    fn into_value(self) -> Value {
        match self {
            Bson::Null => Value::Null,
            Bson::Double(num) => Value::F64(num),
            Bson::String(string) => Value::Str(string),
            Bson::Array(array) => {
                let values = array.into_iter().map(|bson| bson.into_value()).collect();

                Value::List(values)
            }
            Bson::Document(d) => Value::Map(
                d.into_iter()
                    .map(|(k, v)| (k.to_string(), v.into_value()))
                    .collect(),
            ),
            Bson::Boolean(b) => Value::Bool(b),
            Bson::RegularExpression(regex) => {
                let pattern = regex.pattern.clone();
                let options = regex.options.clone();
                Value::Str(format!("/{}/{}", pattern, options))
            }
            Bson::JavaScriptCode(code) => Value::Str(code),
            Bson::JavaScriptCodeWithScope(code) => {
                // let mut map = Map::new();
                // for (key, bson) in code.scope {
                //     map.insert(key, Value::try_from(bson).unwrap());
                // }
                // Value::Object(map)
                todo!();
            }
            Bson::Int32(i) => Value::I32(i),
            Bson::Int64(i) => Value::I64(i),
            // Bson::Timestamp(ts) => Value::Timestamp(ts.to_string().into()),
            Bson::Binary(Binary { bytes, .. }) => Value::Bytea(bytes),
            Bson::ObjectId(oid) => Value::Str(oid.to_hex()),
            // Bson::DateTime(dt) => Value::Date(dt),
            Bson::Symbol(sym) => Value::Str(sym),
            // Bson::Decimal128(dec) => Value::Decimal(dec),
            Bson::Undefined => Value::Null,
            Bson::MaxKey => Value::Null,
            Bson::MinKey => Value::Null,
            Bson::DbPointer(_) => todo!("Handle DbPointer type"),
            Bson::Decimal128(decimal128) => {
                let decimal = Decimal::deserialize(decimal128.bytes());

                Value::Decimal(decimal)
            }
            _ => todo!(),
        }
    }

    fn into_value2(self, data_type: &DataType) -> Value {
        match (self, data_type) {
            (Bson::Null, _) => Value::Null,
            (Bson::Double(num), DataType::Float32) => Value::F32(num as f32),
            (Bson::Double(num), _) => Value::F64(num),
            (Bson::String(string), DataType::Inet) => {
                let ip = string.parse().unwrap();

                Value::Inet(ip)
            }
            (Bson::String(string), _) => Value::Str(string),
            (Bson::Array(array), _) => {
                let values = array
                    .into_iter()
                    .map(|bson| bson.into_value2(data_type))
                    .collect();

                Value::List(values)
            }
            (Bson::Document(d), DataType::Point) => {
                let x = d.get("x").unwrap().as_f64().unwrap();
                let y = d.get("y").unwrap().as_f64().unwrap();

                Value::Point(Point::new(x, y))
            }
            (Bson::Document(d), _) => Value::Map(
                d.into_iter()
                    .map(|(k, v)| (k.to_string(), v.into_value2(data_type)))
                    .collect(),
            ),
            (Bson::Boolean(b), _) => Value::Bool(b),
            (Bson::RegularExpression(regex), _) => {
                let pattern = regex.pattern.clone();
                let options = regex.options.clone();
                Value::Str(format!("/{}/{}", pattern, options))
            }
            (Bson::JavaScriptCode(code), _) => Value::Str(code),
            (Bson::JavaScriptCodeWithScope(code), _) => {
                // let mut map = Map::new();
                // for (key, bson) in code.scope {
                //     map.insert(key, Value::try_from(bson).unwrap());
                // }
                // Value::Object(map)
                todo!();
            }
            (Bson::Int32(i), DataType::Int8) => Value::I8(i as i8),
            (Bson::Int32(i), DataType::Int16) => Value::I16(i as i16),
            (Bson::Int32(i), DataType::Uint16) => Value::U16(i.try_into().unwrap()),
            (Bson::Int32(i), _) => Value::I32(i),
            (Bson::Int64(i), DataType::Uint32) => Value::U32(i.try_into().unwrap()),
            (Bson::Int64(i), _) => Value::I64(i),
            // (Bson::Timestamp(ts), _)  => Value::Timestamp(ts.to_string().into()),
            (Bson::Binary(Binary { bytes, .. }), DataType::Uuid) => {
                let u128 = u128::from_be_bytes(bytes.try_into().unwrap());

                Value::Uuid(u128)
            }
            (Bson::Binary(Binary { bytes, .. }), _) => Value::Bytea(bytes),
            (Bson::ObjectId(oid), _) => Value::Str(oid.to_hex()),
            // (Bson::DateTime(dt), _)  => Value::Date(dt),
            (Bson::Symbol(sym), _) => Value::Str(sym),
            // (Bson::Decimal128(dec), _)  => Value::Decimal(dec),
            (Bson::Undefined, _) => Value::Null,
            (Bson::MaxKey, _) => Value::Null,
            (Bson::MinKey, _) => Value::Null,
            (Bson::DbPointer(_), _) => todo!("Handle DbPointer type"),
            (Bson::Decimal128(decimal128), DataType::Uint128) => {
                let bytes = decimal128.bytes();
                let u128 = u128::from_be_bytes(bytes);

                Value::U128(u128)
            }
            (Bson::Decimal128(decimal128), DataType::Int128) => {
                let bytes = decimal128.bytes();
                let i128 = i128::from_be_bytes(bytes);

                Value::I128(i128)
            }
            (Bson::Decimal128(decimal128), _) => {
                let decimal = Decimal::deserialize(decimal128.bytes());

                Value::Decimal(decimal)
            }
            (Bson::DateTime(dt), DataType::Time) => Value::Time(dt.to_chrono().time()),
            (Bson::DateTime(dt), _) => Value::Date(dt.to_chrono().date_naive()),
            (Bson::Timestamp(dt), _) => {
                Value::Timestamp(NaiveDateTime::from_timestamp_opt(dt.time as i64, 0).unwrap())
            }
        }
    }
}

// pub trait IntoBsonType {
//     fn into_bson_type(&self) -> BsonType;
// }

impl From<&DataType> for BsonType {
    fn from(data_type: &DataType) -> BsonType {
        match data_type {
            DataType::Boolean => BsonType::Boolean,
            DataType::Int8 => BsonType::Int32,
            DataType::Int16 => BsonType::Int32,
            DataType::Int32 => BsonType::Int32,
            DataType::Int => BsonType::Int64,
            DataType::Int128 => BsonType::Decimal128,
            DataType::Uint8 => BsonType::Int32,
            DataType::Uint16 => BsonType::Int32,
            DataType::Uint32 => BsonType::Int64,
            DataType::Uint64 => BsonType::Int64,
            DataType::Uint128 => BsonType::Decimal128,
            DataType::Float32 => BsonType::Double,
            DataType::Float => BsonType::Double,
            DataType::Text => BsonType::String,
            DataType::Bytea => BsonType::Binary,
            // DataType::Inet => BsonType::String,
            DataType::Date => BsonType::Date,
            DataType::Timestamp => BsonType::Timestamp,
            DataType::Time => BsonType::Date,
            DataType::Uuid => BsonType::Binary,
            DataType::Map => BsonType::Object,
            DataType::List => BsonType::Array,
            DataType::Decimal => BsonType::Decimal128,
            DataType::Point => BsonType::Object,
            DataType::Inet => BsonType::String,
            DataType::Interval => BsonType::Date,
        }
    }
}

// impl From<BsonType> for DataType {
//     fn from(bson_type: BsonType) -> DataType {
//         match bson_type {
//             BsonType::Boolean => DataType::Boolean,
//             BsonType::Int32 => DataType::Int32,
//             BsonType::Int64 => DataType::Int,
//             BsonType::Double => DataType::Float,
//             BsonType::String => DataType::Text,
//             BsonType::Binary => DataType::Bytea,
//             BsonType::Timestamp => DataType::Timestamp,
//             BsonType::Object => DataType::Map,
//             BsonType::Array => DataType::List,
//             BsonType::Decimal128 => DataType::Decimal,
//             BsonType::Undefined => todo!(),
//             BsonType::ObjectId => todo!(),
//             BsonType::Date => DataType::Date,
//             BsonType::Null => todo!(),
//             BsonType::RegularExpression => todo!(),
//             BsonType::DbPointer => todo!(),
//             BsonType::JavaScript => todo!(),
//             BsonType::Symbol => todo!(),
//             BsonType::JavaScriptCodeWithScope => todo!(),
//             BsonType::MinKey => todo!(),
//             BsonType::MaxKey => todo!(),
//         }
//     }
// }

pub trait IntoRow {
    fn into_row(self) -> Result<(Key, DataRow)>;
    fn into_row2<'a>(
        self,
        data_types: impl Iterator<Item = &'a DataType>,
    ) -> Result<(Key, DataRow)>;
}

impl IntoRow for Document {
    fn into_row(self) -> Result<(Key, DataRow)> {
        let key = self.get_object_id("_id").unwrap();
        let key_bytes = key.bytes().to_vec();
        let key = Key::Bytea(key_bytes);

        let row = self
            .into_iter()
            .filter(|(key, _)| key != "_id")
            .map(|(_, bson)| bson.into_value())
            .collect::<Vec<_>>();

        Ok((key, DataRow::Vec(row)))
    }

    fn into_row2<'a>(
        self,
        data_types: impl Iterator<Item = &'a DataType>,
    ) -> Result<(Key, DataRow)> {
        let key = self.get_object_id("_id").unwrap();
        let key_bytes = key.bytes().to_vec();
        let key = Key::Bytea(key_bytes);

        let row = self
            .into_iter()
            .skip(1)
            .zip(data_types)
            .map(|((_, bson), data_type)| bson.into_value2(data_type))
            .collect::<Vec<_>>();

        Ok((key, DataRow::Vec(row)))
    }
}

pub trait IntoBson {
    fn into_bson(self) -> Result<Bson>;
}

pub fn into_object_id(key: Key) -> Bson {
    println!("key: {:?}", key);

    match key {
        Key::Str(str) => Bson::ObjectId(bson::oid::ObjectId::from_str(&str).unwrap()),
        Key::Bytea(bytes) => {
            if bytes.len() != 12 {
                todo!();
                // Err(Error::InvalidHexStringLength {
                //     length: s.len(),
                //     hex: s.to_string(),
                // })
            } else {
                let mut byte_array: [u8; 12] = [0; 12];
                byte_array[..].copy_from_slice(&bytes[..]);

                Bson::ObjectId(bson::oid::ObjectId::from_bytes(byte_array))
            }
        }
        Key::U8(val) => Bson::ObjectId(bson::oid::ObjectId::from([val; 12])),
        _ => todo!(),
    }
}

impl IntoBson for Key {
    fn into_bson(self) -> Result<Bson> {
        match self {
            Key::I8(val) => Ok(Bson::Int32(val as i32)),
            Key::I16(val) => Ok(Bson::Int32(val as i32)),
            Key::I32(val) => Ok(Bson::Int32(val)),
            Key::I64(val) => Ok(Bson::Int64(val)),
            Key::U8(val) => Ok(Bson::Int32(val as i32)),
            Key::U16(val) => Ok(Bson::Int32(val as i32)),
            Key::U32(val) => Ok(Bson::Int64(val as i64)),
            Key::U64(val) => Ok(Bson::Int64(val as i64)),
            Key::U128(val) => Ok(Bson::Int64(val as i64)),
            // Key::F32(val) => Ok(Bson::Double(val.into())),
            Key::F64(val) => Ok(Bson::Double(val.into())),
            Key::Decimal(val) => Ok(Bson::String(val.to_string())),
            Key::Bool(val) => Ok(Bson::Boolean(val)),
            Key::Str(val) => Ok(Bson::String(val)),
            Key::Bytea(bytes) => Ok(into_object_id(Key::Bytea(bytes))),
            // Key::Date(val) => Ok(Bson::UtcDatetime(val.and_hms(0, 0, 0))),
            // Key::Timestamp(val) => Ok(Bson::UtcDatetime(val)),
            Key::Time(val) => Ok(Bson::String(val.format("%H:%M:%S%.f").to_string())),
            // Key::Interval(val) => Ok(Bson::String(val.to_string())),
            // Key::Uuid(val) => Ok(Bson::Binary(
            //     bson::spec::BinarySubtype::Uuid,
            //     val.to_be_bytes().to_vec(),
            // )),
            Key::Inet(val) => Ok(Bson::String(val.to_string())),
            Key::None => Ok(Bson::Null),
            k => {
                println!("key: {:?}", k);
                todo!()
            }
        }
    }
}

impl IntoBson for Value {
    fn into_bson(self) -> Result<Bson> {
        match self {
            Value::Null => Ok(Bson::Null),
            Value::I32(val) => Ok(Bson::Int32(val)),
            Value::I64(val) => Ok(Bson::Int64(val)),
            Value::F64(val) => Ok(Bson::Double(val)),
            Value::Bool(val) => Ok(Bson::Boolean(val)),
            Value::Str(val) => Ok(Bson::String(val)),
            Value::List(val) => {
                let bson = val
                    .into_iter()
                    .map(|val| val.into_bson())
                    .collect::<Result<Vec<_>>>()?;

                Ok(Bson::Array(bson))
            }
            Value::Bytea(bytes) => Ok(Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes,
            })),
            Value::Decimal(decimal) => {
                // to_bson(&decimal.serialize()).map_storage_err()

                Ok(Bson::Decimal128(Decimal128::from_bytes(
                    decimal.serialize(),
                )))
            }
            Value::I8(val) => Ok(Bson::Int32(val.into())),
            Value::F32(val) => Ok(Bson::Double(val.into())),
            Value::Uuid(val) => Ok(Bson::Binary(Binary {
                subtype: bson::spec::BinarySubtype::Uuid,

                bytes: val.to_be_bytes().to_vec(),
            })),
            Value::Date(val) => {
                let utc = Utc.from_utc_datetime(&val.and_hms_opt(0, 0, 0).unwrap());
                let datetime = DateTime::from_chrono(utc);

                Ok(Bson::DateTime(datetime))
            }
            Value::Timestamp(val) => {
                let ts = val.timestamp().to_le();

                let increment = match ts {
                    0 => 1, // if time and increment is 0, it sets now()
                    _ => (ts & 0xFFFF_FFFF) as u32,
                };

                let timestamp = Timestamp {
                    time: ((u64::try_from(val.timestamp()).unwrap()) >> 32) as u32,
                    increment,
                };

                Ok(Bson::Timestamp(timestamp))
            }
            Value::Time(val) => {
                let date = NaiveDate::from_ymd(1970, 1, 1);
                let utc = Utc.from_utc_datetime(&NaiveDateTime::new(date, val));
                let datetime = DateTime::from_chrono(utc);

                Ok(Bson::DateTime(datetime))
            }
            Value::Point(Point { x, y }) => Ok(Bson::Document(doc! {  "x": x, "y": y })),
            Value::Inet(val) => Ok(Bson::String(val.to_string())),
            Value::I16(val) => Ok(Bson::Int32(val.into())),
            Value::I128(val) => Ok(Bson::Decimal128(Decimal128::from_bytes(val.to_be_bytes()))),
            Value::Map(hash_map) => {
                let doc = hash_map
                    .into_iter()
                    .fold(Document::new(), |mut acc, (key, value)| {
                        acc.extend(doc! {key: value.into_bson().unwrap()});

                        acc
                    });

                Ok(Bson::Document(doc))
            }
            Value::U32(val) => Ok(Bson::Int64(val.into())),
            Value::U16(val) => Ok(Bson::Int32(val.into())),
            Value::U128(val) => Ok(Bson::Decimal128(Decimal128::from_bytes(val.to_be_bytes()))),
            _ => {
                println!("value: {:?}", self);
                todo!()
            }
        }
    }
}

impl IntoBson for DataRow {
    fn into_bson(self) -> Result<Bson> {
        match self {
            DataRow::Vec(row) => {
                let bson = row
                    .into_iter()
                    .map(|val| val.into_bson())
                    .collect::<Result<Vec<_>>>()?;

                Ok(Bson::Array(bson))
            }
            _ => todo!(), // DataRow::Map(row) => {
                          //     let bson = row
                          //         .into_iter()
                          //         .map(|(key, val)| Ok((key, val.into_bson()?)))
                          //         .collect::<Result<Vec<_>>>()?;

                          //     let doc = doc! { bson };
                          //     Ok(Bson::Document(doc))
                          // }
        }
    }
}

// pub fn into_bson_key(row: DataRow, key: Key) -> Result<Bson> {
//     match row {
//         DataRow::Vec(row) => {
//             let bson = row.into_iter().map(|val| val.into_bson());

//             let bson = vec![Ok(bson!({"_id": key.into_bson()?}))]
//                 .into_iter()
//                 .chain(bson)
//                 .collect::<Result<Vec<_>>>()?;

//             Ok(Bson::Array(bson))
//         }
//         _ => todo!(),
//     }
// }

#[derive(IntoStaticStr, EnumString)]
pub enum BsonType {
    #[strum(to_string = "double")]
    Double,
    #[strum(to_string = "string")]
    String,
    #[strum(to_string = "object")]
    Object,
    #[strum(to_string = "array")]
    Array,
    #[strum(to_string = "binData")]
    Binary,
    #[strum(to_string = "undefined")]
    Undefined,
    #[strum(to_string = "objectId")]
    ObjectId,
    #[strum(to_string = "bool")]
    Boolean,
    #[strum(to_string = "date")]
    Date,
    #[strum(to_string = "null")]
    Null,
    #[strum(to_string = "regex")]
    RegularExpression,
    #[strum(to_string = "dbPointer")]
    DbPointer,
    #[strum(to_string = "javascript")]
    JavaScript,
    #[strum(to_string = "symbol")]
    Symbol,
    #[strum(to_string = "javascriptWithScope")]
    JavaScriptCodeWithScope,
    #[strum(to_string = "int")]
    Int32,
    #[strum(to_string = "timestamp")]
    Timestamp,
    #[strum(to_string = "long")]
    Int64,
    #[strum(to_string = "decimal")]
    Decimal128,
    #[strum(to_string = "minKey")]
    MinKey,
    #[strum(to_string = "maxKey")]
    MaxKey,
}
