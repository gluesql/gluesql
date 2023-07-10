use gluesql_core::{prelude::Key, store::DataRow};
use mongodb::bson::{bson, doc, Bson, Document};

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

pub trait IntoRow {
    fn into_row(self) -> Result<(Key, DataRow)>;
}

impl IntoRow for Document {
    fn into_row(self) -> Result<(Key, DataRow)> {
        // let doc = self.map_storage_err()?;

        let key = self.get_object_id("_id").map_storage_err()?;
        let key_bytes = key.bytes();
        let key_u8 = u8::from_be_bytes(key_bytes[..1].try_into().unwrap()); // TODO: should be string?
        let key = Key::U8(key_u8);

        let row = self
            .into_iter()
            .map(|(_, bson)| bson.into_value())
            .collect::<Vec<_>>();

        Ok((key, DataRow::Vec(row)))
    }
}

pub trait IntoBson {
    fn into_bson(self) -> Result<Bson>;
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
