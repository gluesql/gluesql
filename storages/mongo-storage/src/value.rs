use std::{
    collections::HashMap,
    fmt::{self, Display},
    io::{Bytes, Read},
    str::FromStr,
};

use gluesql_core::{
    prelude::{DataType, Key},
    store::DataRow,
};
use mongodb::bson::{
    self, bson, de, doc, spec::ElementType, to_bson, Binary, Bson, Decimal128, Document,
};
use rust_decimal::Decimal;
use strum_macros::{Display, EnumString, IntoStaticStr};

use crate::error::ResultExt;

use {gluesql_core::data::Value, gluesql_core::prelude::Result};

pub trait IntoValue {
    fn into_value(self) -> Value;
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
            DataType::Int128 => BsonType::Int64,
            DataType::Uint8 => BsonType::Int32,
            DataType::Uint16 => BsonType::Int32,
            DataType::Uint32 => BsonType::Int64,
            DataType::Uint64 => BsonType::Int64,
            DataType::Uint128 => BsonType::Int64,
            DataType::Float32 => BsonType::Double,
            DataType::Float => BsonType::Double,
            DataType::Text => BsonType::String,
            DataType::Bytea => BsonType::Binary,
            DataType::Inet => BsonType::String,
            DataType::Date => BsonType::Timestamp,
            DataType::Timestamp => BsonType::Timestamp,
            DataType::Time => BsonType::String,
            DataType::Interval => BsonType::String,
            DataType::Uuid => BsonType::Binary,
            DataType::Map => BsonType::Object,
            DataType::List => BsonType::Array,
            DataType::Decimal => BsonType::Decimal128,
            DataType::Point => BsonType::String,
        }
    }
}

impl From<BsonType> for DataType {
    fn from(bson_type: BsonType) -> DataType {
        match bson_type {
            BsonType::Boolean => DataType::Boolean,
            BsonType::Int32 => DataType::Int32,
            BsonType::Int64 => DataType::Int,
            BsonType::Double => DataType::Float,
            BsonType::String => DataType::Text,
            BsonType::Binary => DataType::Bytea,
            BsonType::Timestamp => DataType::Timestamp,
            BsonType::Object => DataType::Map,
            BsonType::Array => DataType::List,
            BsonType::Decimal128 => DataType::Decimal,
            BsonType::Undefined => todo!(),
            BsonType::ObjectId => todo!(),
            BsonType::Date => todo!(),
            BsonType::Null => todo!(),
            BsonType::RegularExpression => todo!(),
            BsonType::DbPointer => todo!(),
            BsonType::JavaScript => todo!(),
            BsonType::Symbol => todo!(),
            BsonType::JavaScriptCodeWithScope => todo!(),
            BsonType::MinKey => todo!(),
            BsonType::MaxKey => todo!(),
        }
    }
}

pub trait IntoRow {
    fn into_row(self) -> Result<(Key, DataRow)>;
}

impl IntoRow for Document {
    fn into_row(self) -> Result<(Key, DataRow)> {
        // let doc = self.map_storage_err()?;

        println!("err11");
        let key = self.get_object_id("_id").map_storage_err()?;
        let key_bytes = key.bytes().to_vec();
        // let key_u8 = u8::from_be_bytes(key_bytes[..1].try_into().unwrap()); // TODO: should be string?
        let key = Key::Bytea(key_bytes);

        let row = self
            .into_iter()
            .skip(1)
            .map(|(_, bson)| bson.into_value())
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
            Key::I128(val) => Ok(Bson::Int64(val as i64)),
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
            // Value::Map(val) => {
            // let bson = val
            //     .into_iter()
            //     .map(|(key, val)| Ok((key, val.into_bson()?)))
            //     .collect::<Result<Vec<_>>>()?;

            // let doc = doc! { bson };
            // Ok(Bson::Document(doc))
            // }
            _ => todo!(),
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
