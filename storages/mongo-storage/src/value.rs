use std::{
    collections::HashMap,
    fmt,
    io::{Bytes, Read},
    str::FromStr,
};

use gluesql_core::{
    prelude::{DataType, Key},
    store::DataRow,
};
use mongodb::bson::{self, bson, doc, spec::ElementType, Bson, Document};

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
            // Bson::Binary(bin) => Value::Binary(bin.bytes().to_vec()),
            Bson::ObjectId(oid) => Value::Str(oid.to_hex()),
            // Bson::DateTime(dt) => Value::Date(dt),
            Bson::Symbol(sym) => Value::Str(sym),
            // Bson::Decimal128(dec) => Value::Decimal(dec),
            Bson::Undefined => Value::Null,
            Bson::MaxKey => Value::Null,
            Bson::MinKey => Value::Null,
            Bson::DbPointer(_) => todo!("Handle DbPointer type"),
            _ => todo!(),
        }
    }
}

pub trait IntoBsonType {
    fn into_bson_type(&self) -> ElementType;
}

impl IntoBsonType for DataType {
    fn into_bson_type(&self) -> ElementType {
        match &self {
            DataType::Boolean => ElementType::Boolean,
            DataType::Int8 => ElementType::Int32,
            DataType::Int16 => ElementType::Int32,
            DataType::Int32 => ElementType::Int32,
            DataType::Int => ElementType::Int64,
            DataType::Int128 => ElementType::Int64,
            DataType::Uint8 => ElementType::Int32,
            DataType::Uint16 => ElementType::Int32,
            DataType::Uint32 => ElementType::Int64,
            DataType::Uint64 => ElementType::Int64,
            DataType::Uint128 => ElementType::Int64,
            DataType::Float32 => ElementType::Double,
            DataType::Float => ElementType::Double,
            DataType::Text => ElementType::String,
            DataType::Bytea => ElementType::Binary,
            DataType::Inet => ElementType::String,
            DataType::Date => ElementType::Timestamp,
            DataType::Timestamp => ElementType::Timestamp,
            DataType::Time => ElementType::String,
            DataType::Interval => ElementType::String,
            DataType::Uuid => ElementType::Binary,
            DataType::Map => ElementType::EmbeddedDocument,
            DataType::List => ElementType::Array,
            DataType::Decimal => ElementType::String,
            DataType::Point => ElementType::String,
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
            // Key::Bytea(val) => Ok(Bson::Binary(bson::spec::BinarySubtype::Generic, val)),
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
            _ => todo!(),
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

pub fn into_bson_key(row: DataRow, key: Key) -> Result<Bson> {
    match row {
        DataRow::Vec(row) => {
            let bson = row.into_iter().map(|val| val.into_bson());

            let bson = vec![Ok(bson!({"_id": key.into_bson()?}))]
                .into_iter()
                .chain(bson)
                .collect::<Result<Vec<_>>>()?;

            Ok(Bson::Array(bson))
        }
        _ => todo!(),
    }
}
pub fn get_element_type_string(element_type: ElementType) -> &'static str {
    match element_type {
        ElementType::Double => "double",
        ElementType::String => "string",
        ElementType::Array => "array",
        ElementType::Binary => "binary",
        ElementType::Undefined => "undefined",
        ElementType::ObjectId => "objectId",
        ElementType::Boolean => "boolean",
        ElementType::DateTime => "datetime",
        ElementType::Null => "null",
        ElementType::RegularExpression => "regularexpression",
        ElementType::JavaScriptCode => "javascriptcode",
        ElementType::Symbol => "symbol",
        ElementType::JavaScriptCodeWithScope => "javascriptcodewithscope",
        ElementType::Int32 => "int",
        ElementType::Timestamp => "timestamp",
        ElementType::Int64 => "long",
        ElementType::Decimal128 => "decimal128",
        ElementType::MinKey => "minkey",
        ElementType::MaxKey => "maxkey",
        ElementType::DbPointer => "dbpointer",
        ElementType::EmbeddedDocument => "embeddeddocument",
    }
}
