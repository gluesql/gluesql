use chrono::{NaiveDateTime, TimeZone};
use gluesql_core::{
    ast::{Expr, ToSql},
    chrono::{NaiveDate, Utc},
    data::{Interval, Point},
    parse_sql::parse_interval,
    prelude::DataType,
    translate::translate_expr,
};
use mongodb::bson::{self, doc, Binary, Bson, DateTime, Decimal128, Document};
use rust_decimal::Decimal;
use {gluesql_core::data::Value, gluesql_core::prelude::Result};

pub trait IntoValue {
    fn into_value_schemaless(self) -> Value;
    fn into_value(self, data_type: &DataType) -> Value;
}

impl IntoValue for Bson {
    fn into_value_schemaless(self) -> Value {
        match self {
            Bson::Null => Value::Null,
            Bson::Double(num) => Value::F64(num),
            Bson::String(string) => Value::Str(string),
            Bson::Array(array) => {
                let values = array
                    .into_iter()
                    .map(|bson| bson.into_value_schemaless())
                    .collect();

                Value::List(values)
            }
            Bson::Document(d) => Value::Map(
                d.into_iter()
                    .map(|(k, v)| (k.to_string(), v.into_value_schemaless()))
                    .collect(),
            ),
            Bson::Boolean(b) => Value::Bool(b),
            Bson::RegularExpression(regex) => {
                let pattern = regex.pattern.clone();
                let options = regex.options.clone();
                Value::Str(format!("/{}/{}", pattern, options))
            }
            Bson::JavaScriptCode(code) => Value::Str(code),
            Bson::Int32(i) => Value::I32(i),
            Bson::Int64(i) => Value::I64(i),
            Bson::Binary(Binary { bytes, .. }) => Value::Bytea(bytes),
            Bson::ObjectId(oid) => Value::Str(oid.to_hex()),
            Bson::Symbol(sym) => Value::Str(sym),
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

    fn into_value(self, data_type: &DataType) -> Value {
        match (self, data_type) {
            (Bson::Null, _) => Value::Null,
            (Bson::Double(num), DataType::Float32) => Value::F32(num as f32),
            (Bson::Double(num), _) => Value::F64(num),
            (Bson::String(string), DataType::Inet) => {
                let ip = string.parse().unwrap();

                Value::Inet(ip)
            }
            (Bson::String(string), DataType::Timestamp) => {
                //2021-10-13 06:42:40.364832862
                Value::Timestamp(
                    NaiveDateTime::parse_from_str(&string, "%Y-%m-%d %H:%M:%S%.f").unwrap(),
                )
            }
            (Bson::String(string), DataType::Interval) => {
                let interval = parse_interval(&string).unwrap();
                let interval = translate_expr(&interval).unwrap();
                let interval = match interval {
                    Expr::Interval {
                        expr,
                        leading_field,
                        last_field,
                    } => Value::Interval(
                        Interval::try_from_str(&expr.to_sql(), leading_field, last_field).unwrap(),
                    ),
                    _ => unreachable!(),
                };
                // let interval = Interval::try_from_str(&string, None, None).unwrap();

                interval
                // Value::Interval(interval)
            }
            (Bson::String(string), _) => Value::Str(string),
            (Bson::Array(array), _) => {
                let values = array
                    .into_iter()
                    .map(|bson| bson.into_value(data_type))
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
                    .map(|(k, v)| (k.to_string(), v.into_value(data_type)))
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
            (Bson::Int32(i), DataType::Uint8) => Value::U8(i.try_into().unwrap()),
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
            (Bson::Decimal128(decimal128), DataType::Uint64) => {
                let bytes = decimal128.bytes();
                let u64 = u64::from_be_bytes(bytes[..8].try_into().unwrap());

                Value::U64(u64)
            }
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
                unimplemented!()
                // let increment = match dt.time {
                //     0 => 0,
                //     _ => dt.increment,
                // };

                // Value::Timestamp(
                //     NaiveDateTime::from_timestamp_opt(dt.time as i64, increment).unwrap(),
                // )
            }
        }
    }
}

pub trait IntoBson {
    fn into_bson(self) -> Result<Bson>;
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
            Value::Decimal(decimal) => Ok(Bson::Decimal128(Decimal128::from_bytes(
                decimal.serialize(),
            ))),
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
            Value::Timestamp(val) => Ok(Bson::String(val.to_string())),
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
            Value::U8(val) => Ok(Bson::Int32(val.into())),
            Value::U64(val) => {
                // TODO: this is a hack, but it works for now
                let mut my_new_bytes: [u8; 16] = [0; 16];
                my_new_bytes[..8].copy_from_slice(&val.to_be_bytes());

                Ok(Bson::Decimal128(Decimal128::from_bytes(my_new_bytes)))
            }
            Value::Interval(val) => Ok(Bson::String(val.to_sql_str())),
        }
    }
}
