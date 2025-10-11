use {
    crate::error::MongoStorageError,
    gluesql_core::{
        ast::{Expr, ToSql},
        chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc},
        data::{FloatVector, Interval, Point, Value},
        parse_sql::parse_interval,
        prelude::DataType,
        translate::translate_expr,
    },
    mongodb::bson::{self, Binary, Bson, DateTime, Decimal128, Document, doc},
    rust_decimal::Decimal,
    std::collections::BTreeMap,
};

type Result<T> = std::result::Result<T, MongoStorageError>;

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
                    .collect::<Result<BTreeMap<_, _>>>()?,
            ),
            Bson::Boolean(b) => Value::Bool(b),
            Bson::Int32(i) => Value::I32(i),
            Bson::Int64(i) => Value::I64(i),
            Bson::Double(f) => Value::F64(f),
            Bson::Array(arr) => Value::List(
                arr.into_iter()
                    .map(|v| v.into_value_schemaless())
                    .collect::<Result<Vec<_>>>()?,
            ),
            Bson::Null => Value::Null,
            _ => {
                return Err(MongoStorageError::UnsupportedBsonType);
            }
        })
    }

    fn into_value(self, data_type: &DataType) -> Result<Value> {
        Ok(match (self, data_type) {
            (Bson::Null, _) => Value::Null,
            (Bson::Double(num), DataType::Float32) => Value::F32(num as f32),
            (Bson::Double(num), _) => Value::F64(num),
            (Bson::String(string), DataType::Inet) => Value::Inet(
                string
                    .parse()
                    .map_err(|_| MongoStorageError::UnsupportedBsonType)?,
            ),
            (Bson::String(string), DataType::Timestamp) => Value::Timestamp(
                NaiveDateTime::parse_from_str(&string, "%Y-%m-%d %H:%M:%S%.f")
                    .map_err(|_| MongoStorageError::UnsupportedBsonType)?,
            ),
            (Bson::String(string), DataType::Interval) => {
                let interval =
                    parse_interval(string).map_err(|_| MongoStorageError::UnsupportedBsonType)?;
                let interval = translate_expr(&interval)
                    .map_err(|_| MongoStorageError::UnsupportedBsonType)?;
                match interval {
                    Expr::Interval {
                        expr,
                        leading_field,
                        last_field,
                    } => Value::Interval(
                        Interval::try_from_str(&expr.to_sql(), leading_field, last_field)
                            .map_err(|_| MongoStorageError::UnsupportedBsonType)?,
                    ),
                    _ => {
                        return Err(MongoStorageError::UnsupportedBsonType);
                    }
                }
            }
            (Bson::String(string), _) => Value::Str(string),
            (Bson::Array(array), DataType::FloatVector) => {
                let floats: Result<Vec<f32>> = array
                    .into_iter()
                    .map(|bson| match bson {
                        Bson::Double(f) => Ok(f as f32),
                        Bson::Int32(i) => Ok(i as f32),
                        Bson::Int64(i) => Ok(i as f32),
                        _ => Err(MongoStorageError::UnsupportedBsonType),
                    })
                    .collect();

                let floats = floats?;
                let float_vector =
                    FloatVector::new(floats).map_err(|_| MongoStorageError::UnsupportedBsonType)?;
                Value::FloatVector(float_vector)
            }
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
                    .ok_or(MongoStorageError::UnsupportedBsonType)?;
                let y = d
                    .get("y")
                    .and_then(Bson::as_f64)
                    .ok_or(MongoStorageError::UnsupportedBsonType)?;

                Value::Point(Point::new(x, y))
            }
            (Bson::Document(d), _) => Value::Map(
                d.into_iter()
                    .map(|(k, v)| Ok((k, v.into_value(data_type)?)))
                    .collect::<Result<BTreeMap<_, _>>>()?,
            ),
            (Bson::Boolean(b), _) => Value::Bool(b),
            (Bson::RegularExpression(regex), _) => {
                let pattern = regex.pattern;
                let options = regex.options;
                Value::Str(format!("/{pattern}/{options}"))
            }
            (Bson::Int32(i), DataType::Uint8) => Value::U8(
                i.try_into()
                    .map_err(|_| MongoStorageError::UnsupportedBsonType)?,
            ),
            (Bson::Int32(i), DataType::Int8) => Value::I8(i as i8),
            (Bson::Int32(i), DataType::Int16) => Value::I16(i as i16),
            (Bson::Int32(i), DataType::Uint16) => Value::U16(
                i.try_into()
                    .map_err(|_| MongoStorageError::UnsupportedBsonType)?,
            ),
            (Bson::Int32(i), _) => Value::I32(i),
            (Bson::Int64(i), DataType::Uint32) => Value::U32(
                i.try_into()
                    .map_err(|_| MongoStorageError::UnsupportedBsonType)?,
            ),
            (Bson::Int64(i), _) => Value::I64(i),
            (Bson::Binary(Binary { bytes, .. }), DataType::Uuid) => {
                let u128 = u128::from_be_bytes(
                    bytes
                        .try_into()
                        .map_err(|_| MongoStorageError::UnsupportedBsonType)?,
                );

                Value::Uuid(u128)
            }
            (Bson::Binary(Binary { bytes, .. }), _) => Value::Bytea(bytes),
            (Bson::Decimal128(decimal128), DataType::Uint64) => {
                let bytes = decimal128.bytes();
                let u64 = u64::from_be_bytes(
                    bytes[..8]
                        .try_into()
                        .map_err(|_| MongoStorageError::UnsupportedBsonType)?,
                );

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
                Value::Map(BTreeMap::from([
                    ("code".to_owned(), Value::Str(code)),
                    (
                        "scope".to_owned(),
                        bson::to_bson(&scope)
                            .map_err(|_| MongoStorageError::UnsupportedBsonType)?
                            .into_value_schemaless()?,
                    ),
                ]))
            }
            (Bson::MinKey, _) => Value::Str("MinKey()".to_owned()),
            (Bson::MaxKey, _) => Value::Str("MaxKey()".to_owned()),
            _ => {
                return Err(MongoStorageError::UnsupportedBsonType);
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
                        .ok_or(MongoStorageError::UnsupportedBsonType)?,
                );
                let datetime = DateTime::from_chrono(utc);

                Ok(Bson::DateTime(datetime))
            }
            Value::Timestamp(val) => Ok(Bson::String(val.to_string())),
            Value::Time(val) => {
                let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .ok_or(MongoStorageError::UnsupportedBsonType)?;
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

                            Ok::<_, MongoStorageError>(acc)
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
            Value::FloatVector(val) => {
                let bson = val
                    .data()
                    .iter()
                    .map(|&f| Bson::Double(f.into()))
                    .collect::<Vec<_>>();

                Ok(Bson::Array(bson))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{IntoValue, IntoBson},
        gluesql_core::{ast::DataType, data::{FloatVector, Value}},
        mongodb::bson::{Bson, Binary},
    };

    #[test]
    fn float_vector_bson_to_value() {
        // Test BSON Array with Double values to FloatVector
        let bson_array = Bson::Array(vec![
            Bson::Double(1.0),
            Bson::Double(2.5),
            Bson::Double(3.0),
        ]);
        
        let result = bson_array.into_value(&DataType::FloatVector).unwrap();
        if let Value::FloatVector(vec) = result {
            assert_eq!(vec.data(), &[1.0, 2.5, 3.0]);
            assert_eq!(vec.dimension(), 3);
        } else {
            panic!("Expected FloatVector value");
        }

        // Test BSON Array with mixed Int32, Int64, Double to FloatVector
        let bson_mixed = Bson::Array(vec![
            Bson::Int32(1),
            Bson::Double(2.5),
            Bson::Int64(3),
        ]);
        
        let result = bson_mixed.into_value(&DataType::FloatVector).unwrap();
        if let Value::FloatVector(vec) = result {
            assert_eq!(vec.data(), &[1.0, 2.5, 3.0]);
        } else {
            panic!("Expected FloatVector value");
        }

        // Test BSON Array with unsupported type should return error
        let bson_invalid = Bson::Array(vec![
            Bson::Double(1.0),
            Bson::String("invalid".to_owned()),
        ]);
        
        let result = bson_invalid.into_value(&DataType::FloatVector);
        assert!(result.is_err());
    }

    #[test]
    fn float_vector_value_to_bson() {
        // Test FloatVector to BSON Array conversion
        let float_vector = FloatVector::new(vec![1.5, 2.5, 3.5]).unwrap();
        let value = Value::FloatVector(float_vector);
        
        let result = value.into_bson().unwrap();
        if let Bson::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Bson::Double(1.5));
            assert_eq!(arr[1], Bson::Double(2.5));
            assert_eq!(arr[2], Bson::Double(3.5));
        } else {
            panic!("Expected BSON Array");
        }

        // Test single element FloatVector
        let single_vector = FloatVector::new(vec![42.0]).unwrap();
        let single_value = Value::FloatVector(single_vector);
        
        let result = single_value.into_bson().unwrap();
        if let Bson::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], Bson::Double(42.0));
        } else {
            panic!("Expected BSON Array with single element");
        }
    }

    #[test]
    fn float_vector_error_cases() {
        // Test empty BSON Array should fail FloatVector creation
        let empty_array = Bson::Array(vec![]);
        let result = empty_array.into_value(&DataType::FloatVector);
        assert!(result.is_err());

        // Test BSON Array with only invalid types
        let invalid_array = Bson::Array(vec![
            Bson::String("not_a_number".to_owned()),
            Bson::Binary(Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![1, 2, 3] }),
        ]);
        let result = invalid_array.into_value(&DataType::FloatVector);
        assert!(result.is_err());
    }
}
