use {
    crate::error::{MongoStorageError, OptionExt, ResultExt},
    gluesql_core::{
        ast::{Expr, ToSql},
        chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc},
        data::{value::ArrayValue, Interval, Point, Value},
        parse_sql::parse_interval,
        prelude::{DataType, Error, Result},
        translate::translate_expr,
    },
    mongodb::bson::{self, doc, Binary, Bson, DateTime, Decimal128, Document},
    rust_decimal::Decimal,
    std::collections::HashMap,
};

pub trait IntoValue {
    fn into_value_schemaless(self) -> Result<Value>;
    fn into_value(self, data_type: &DataType) -> Result<Value>;
}

impl IntoValue for Bson {
    fn into_value_schemaless(self) -> Result<Value> {
        Ok(match self {
            Bson::String(string) => Value::Str(string),
            Bson::Document(d) => Value::Map(
                d.into_iter()
                    .map(|(k, v)| Ok((k, v.into_value_schemaless()?)))
                    .collect::<Result<HashMap<_, _>>>()?,
            ),
            Bson::Boolean(b) => Value::Bool(b),
            Bson::Int32(i) => Value::I32(i),
            Bson::Int64(i) => Value::I64(i),
            _ => {
                return Err(Error::StorageMsg(
                    MongoStorageError::UnsupportedBsonType.to_string(),
                ));
            }
        })
    }

    fn into_value(self, data_type: &DataType) -> Result<Value> {
        Ok(match (self, data_type) {
            (Bson::Null, _) => Value::Null,
            (Bson::Double(num), DataType::Float32) => Value::F32(num as f32),
            (Bson::Double(num), _) => Value::F64(num),
            (Bson::String(string), DataType::Inet) => {
                Value::Inet(string.parse().map_storage_err()?)
            }
            (Bson::String(string), DataType::Timestamp) => Value::Timestamp(
                NaiveDateTime::parse_from_str(&string, "%Y-%m-%d %H:%M:%S%.f").map_storage_err()?,
            ),
            (Bson::String(string), DataType::Interval) => {
                let interval = parse_interval(string)?;
                let interval = translate_expr(&interval)?;
                match interval {
                    Expr::Interval {
                        expr,
                        leading_field,
                        last_field,
                    } => Value::Interval(Interval::try_from_str(
                        &expr.to_sql(),
                        leading_field,
                        last_field,
                    )?),
                    _ => {
                        return Err(Error::StorageMsg(
                            MongoStorageError::UnsupportedBsonType.to_string(),
                        ))
                    }
                }
            }
            (Bson::String(string), _) => Value::Str(string),
            (Bson::Array(array), _) => {
                let values = array
                    .into_iter()
                    .map(|bson| bson.into_value(data_type))
                    .collect::<Result<Vec<_>>>()?;

                Value::List(values)
            }
            (Bson::Document(d), DataType::Point) => {
                let x = d
                    .get("x")
                    .and_then(Bson::as_f64)
                    .map_storage_err(MongoStorageError::UnsupportedBsonType)?;
                let y = d
                    .get("y")
                    .and_then(Bson::as_f64)
                    .map_storage_err(MongoStorageError::UnsupportedBsonType)?;

                Value::Point(Point::new(x, y))
            }
            (Bson::Document(d), _) => Value::Map(
                d.into_iter()
                    .map(|(k, v)| Ok((k, v.into_value(data_type)?)))
                    .collect::<Result<HashMap<_, _>>>()?,
            ),
            (Bson::Boolean(b), _) => Value::Bool(b),
            (Bson::RegularExpression(regex), _) => {
                let pattern = regex.pattern;
                let options = regex.options;
                Value::Str(format!("/{}/{}", pattern, options))
            }
            (Bson::Int32(i), DataType::Uint8) => Value::U8(i.try_into().map_storage_err()?),
            (Bson::Int32(i), DataType::Int8) => Value::I8(i as i8),
            (Bson::Int32(i), DataType::Int16) => Value::I16(i as i16),
            (Bson::Int32(i), DataType::Uint16) => Value::U16(i.try_into().map_storage_err()?),
            (Bson::Int32(i), _) => Value::I32(i),
            (Bson::Int64(i), DataType::Uint32) => Value::U32(i.try_into().map_storage_err()?),
            (Bson::Int64(i), _) => Value::I64(i),
            (Bson::Binary(Binary { bytes, .. }), DataType::Uuid) => {
                let u128 = u128::from_be_bytes(
                    bytes
                        .try_into()
                        .ok()
                        .map_storage_err(MongoStorageError::UnsupportedBsonType)?,
                );

                Value::Uuid(u128)
            }
            (Bson::Binary(Binary { bytes, .. }), _) => Value::Bytea(bytes),
            (Bson::Decimal128(decimal128), DataType::Uint64) => {
                let bytes = decimal128.bytes();
                let u64 = u64::from_be_bytes(bytes[..8].try_into().map_storage_err()?);

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
            (Bson::JavaScriptCode(code), _) => Value::Str(code),
            (Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope { code, scope }), _) => {
                Value::Map(HashMap::from([
                    ("code".to_owned(), Value::Str(code)),
                    (
                        "scope".to_owned(),
                        bson::to_bson(&scope)
                            .map_storage_err()?
                            .into_value_schemaless()?,
                    ),
                ]))
            }
            (Bson::MinKey, _) => Value::Str("MinKey()".to_owned()),
            (Bson::MaxKey, _) => Value::Str("MaxKey()".to_owned()),
            _ => {
                return Err(Error::StorageMsg(
                    MongoStorageError::UnsupportedBsonType.to_string(),
                ));
            }
        })
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
            Value::Array(array_value) => array_value.into_bson(),
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
                let utc = Utc.from_utc_datetime(
                    &val.and_hms_opt(0, 0, 0)
                        .map_storage_err(MongoStorageError::UnsupportedBsonType)?,
                );
                let datetime = DateTime::from_chrono(utc);

                Ok(Bson::DateTime(datetime))
            }
            Value::Timestamp(val) => Ok(Bson::String(val.to_string())),
            Value::Time(val) => {
                let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .map_storage_err(MongoStorageError::UnsupportedBsonType)?;
                let utc = Utc.from_utc_datetime(&NaiveDateTime::new(date, val));
                let datetime = DateTime::from_chrono(utc);

                Ok(Bson::DateTime(datetime))
            }
            Value::Point(Point { x, y }) => Ok(Bson::Document(doc! {  "x": x, "y": y })),
            Value::Inet(val) => Ok(Bson::String(val.to_string())),
            Value::I16(val) => Ok(Bson::Int32(val.into())),
            Value::I128(val) => Ok(Bson::Decimal128(Decimal128::from_bytes(val.to_be_bytes()))),
            Value::Map(hash_map) => {
                let doc =
                    hash_map
                        .into_iter()
                        .try_fold(Document::new(), |mut acc, (key, value)| {
                            acc.extend(doc! {key: value.into_bson()?});

                            Ok::<_, Error>(acc)
                        })?;

                Ok(Bson::Document(doc))
            }
            Value::U32(val) => Ok(Bson::Int64(val.into())),
            Value::U16(val) => Ok(Bson::Int32(val.into())),
            Value::U128(val) => Ok(Bson::Decimal128(Decimal128::from_bytes(val.to_be_bytes()))),
            Value::U8(val) => Ok(Bson::Int32(val.into())),
            Value::U64(val) => {
                let mut bytes_128: [u8; 16] = [0; 16];
                bytes_128[..8].copy_from_slice(&val.to_be_bytes());

                Ok(Bson::Decimal128(Decimal128::from_bytes(bytes_128)))
            }

            Value::Interval(val) => Ok(Bson::String(val.to_sql_str())),
        }
    }
}

fn vec_to_bson<T>(val: Vec<T>, bsonizer: fn(T) -> Bson) -> Result<Bson> {
    Ok(Bson::Array(val.into_iter().map(bsonizer).collect()))
}

fn try_vec_to_bson<T>(val: Vec<T>, bsonizer: fn(T) -> Result<Bson>) -> Result<Bson> {
    val.into_iter()
        .map(bsonizer)
        .collect::<Result<Vec<_>>>()
        .map(Bson::Array)
}

impl IntoBson for ArrayValue {
    fn into_bson(self) -> Result<Bson> {
        match self {
            ArrayValue::I8(val) => vec_to_bson(val, |val| Bson::Int32(val.into())),
            ArrayValue::I16(val) => vec_to_bson(val, |val| Bson::Int32(val.into())),
            ArrayValue::I32(val) => vec_to_bson(val, Bson::Int32),
            ArrayValue::I64(val) => vec_to_bson(val, Bson::Int64),
            ArrayValue::I128(val) => vec_to_bson(val, |val| {
                Bson::Decimal128(Decimal128::from_bytes(val.to_be_bytes()))
            }),
            ArrayValue::U8(val) => vec_to_bson(val, |val| Bson::Int32(val.into())),
            ArrayValue::U16(val) => vec_to_bson(val, |val| Bson::Int32(val.into())),
            ArrayValue::U32(val) => vec_to_bson(val, |val| Bson::Int64(val.into())),
            ArrayValue::U64(val) => vec_to_bson(val, |val| {
                let mut bytes_128: [u8; 16] = [0; 16];
                bytes_128[..8].copy_from_slice(&val.to_be_bytes());

                Bson::Decimal128(Decimal128::from_bytes(bytes_128))
            }),
            ArrayValue::U128(val) => vec_to_bson(val, |val| {
                Bson::Decimal128(Decimal128::from_bytes(val.to_be_bytes()))
            }),
            ArrayValue::F32(val) => vec_to_bson(val, |val| Bson::Double(val.into())),
            ArrayValue::F64(val) => vec_to_bson(val, Bson::Double),
            ArrayValue::Decimal(decimal) => vec_to_bson(decimal, |val| {
                Bson::Decimal128(Decimal128::from_bytes(val.serialize()))
            }),
            ArrayValue::Bool(val) => vec_to_bson(val, Bson::Boolean),
            ArrayValue::Str(val) => vec_to_bson(val, Bson::String),
            ArrayValue::Bytea(bytes) => vec_to_bson(bytes, |val| {
                Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: val,
                })
            }),
            ArrayValue::Uuid(val) => vec_to_bson(val, |val| {
                Bson::Binary(Binary {
                    subtype: bson::spec::BinarySubtype::Uuid,
                    bytes: val.to_be_bytes().to_vec(),
                })
            }),
            ArrayValue::Date(vals) => try_vec_to_bson(vals, |val| {
                let utc = Utc.from_utc_datetime(
                    &val.and_hms_opt(0, 0, 0)
                        .map_storage_err(MongoStorageError::UnsupportedBsonType)?,
                );
                let datetime = DateTime::from_chrono(utc);

                Ok(Bson::DateTime(datetime))
            }),
            ArrayValue::Timestamp(val) => vec_to_bson(val, |val| Bson::String(val.to_string())),
            ArrayValue::Time(val) => try_vec_to_bson(val, |val| {
                let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .map_storage_err(MongoStorageError::UnsupportedBsonType)?;
                let utc = Utc.from_utc_datetime(&NaiveDateTime::new(date, val));
                let datetime = DateTime::from_chrono(utc);

                Ok(Bson::DateTime(datetime))
            }),
            ArrayValue::Point(val) => vec_to_bson(val, |Point { x, y }| {
                Bson::Document(doc! {  "x": x, "y": y })
            }),
            ArrayValue::Inet(val) => vec_to_bson(val, |val| Bson::String(val.to_string())),
            ArrayValue::Map(hash_map) => try_vec_to_bson(hash_map, |hash_map| {
                let doc =
                    hash_map
                        .into_iter()
                        .try_fold(Document::new(), |mut acc, (key, value)| {
                            acc.extend(doc! {key: value.into_bson()?});

                            Ok::<_, Error>(acc)
                        })?;

                Ok(Bson::Document(doc))
            }),

            ArrayValue::Interval(val) => vec_to_bson(val, |val| Bson::String(val.to_sql_str())),
            ArrayValue::List(val) => val
                .into_iter()
                .map(|val| {
                    val.into_iter()
                        .map(IntoBson::into_bson)
                        .collect::<Result<Vec<_>>>()
                        .map(Bson::Array)
                })
                .collect::<Result<Vec<_>>>()
                .map(Bson::Array),
            ArrayValue::Array(array_values) => array_values
                .into_iter()
                .map(IntoBson::into_bson)
                .collect::<Result<Vec<_>>>()
                .map(Bson::Array),
        }
    }
}
