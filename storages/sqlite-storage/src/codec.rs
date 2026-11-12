use {
    crate::{Result, SqliteStorageError},
    gluesql_core::{ast::DataType, data::Value},
    rusqlite::types::{Value as SqlValue, ValueRef},
};

pub(super) fn physical_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int
        | DataType::Uint8
        | DataType::Uint16
        | DataType::Uint32 => "INTEGER",
        DataType::Float32 | DataType::Float => "REAL",
        DataType::Bytea => "BLOB",
        DataType::Int128
        | DataType::Uint64
        | DataType::Uint128
        | DataType::Text
        | DataType::Inet
        | DataType::Date
        | DataType::Timestamp
        | DataType::Time
        | DataType::Interval
        | DataType::Uuid
        | DataType::Map
        | DataType::List
        | DataType::Decimal
        | DataType::Point => "TEXT",
    }
}

pub(super) fn encode(value: &Value, data_type: &DataType) -> Result<SqlValue> {
    value.validate_type(data_type)?;

    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Bool(v) => SqlValue::Integer(i64::from(*v)),
        Value::I8(v) => SqlValue::Integer(i64::from(*v)),
        Value::I16(v) => SqlValue::Integer(i64::from(*v)),
        Value::I32(v) => SqlValue::Integer(i64::from(*v)),
        Value::I64(v) => SqlValue::Integer(*v),
        Value::U8(v) => SqlValue::Integer(i64::from(*v)),
        Value::U16(v) => SqlValue::Integer(i64::from(*v)),
        Value::U32(v) => SqlValue::Integer(i64::from(*v)),

        Value::F32(v) => encode_float(f64::from(*v))?,
        Value::F64(v) => encode_float(*v)?,
        Value::Bytea(v) => SqlValue::Blob(v.clone()),

        Value::Map(_) | Value::List(_) | Value::Interval(_) | Value::Point(_) => {
            SqlValue::Text(ron::to_string(value)?)
        }
        Value::I128(_)
        | Value::U64(_)
        | Value::U128(_)
        | Value::Str(_)
        | Value::Inet(_)
        | Value::Date(_)
        | Value::Timestamp(_)
        | Value::Time(_)
        | Value::Uuid(_)
        | Value::Decimal(_) => SqlValue::Text(String::from(value)),
    })
}

fn encode_float(value: f64) -> Result<SqlValue> {
    if value.is_nan() {
        return Err(SqliteStorageError::NanNotSupported);
    }

    Ok(SqlValue::Real(value))
}

pub(super) fn decode(value: ValueRef<'_>, data_type: &DataType) -> Result<Value> {
    if value == ValueRef::Null {
        return Ok(Value::Null);
    }

    match (data_type, value) {
        (DataType::Boolean, ValueRef::Integer(v)) => match v {
            0 => Ok(Value::Bool(false)),
            1 => Ok(Value::Bool(true)),
            _ => Err(SqliteStorageError::InvalidBoolean(v)),
        },
        (DataType::Int, ValueRef::Integer(v)) => Ok(Value::I64(v)),
        (
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Uint8
            | DataType::Uint16
            | DataType::Uint32,
            ValueRef::Integer(v),
        ) => {
            let out_of_range = || SqliteStorageError::IntegerOutOfRange {
                data_type: data_type.clone(),
                value: v,
            };

            Ok(match data_type {
                DataType::Int8 => Value::I8(i8::try_from(v).map_err(|_| out_of_range())?),
                DataType::Int16 => Value::I16(i16::try_from(v).map_err(|_| out_of_range())?),
                DataType::Int32 => Value::I32(i32::try_from(v).map_err(|_| out_of_range())?),
                DataType::Uint8 => Value::U8(u8::try_from(v).map_err(|_| out_of_range())?),
                DataType::Uint16 => Value::U16(u16::try_from(v).map_err(|_| out_of_range())?),
                _ => Value::U32(u32::try_from(v).map_err(|_| out_of_range())?),
            })
        }

        (DataType::Float32, ValueRef::Real(v)) => Ok(Value::F32(v as f32)),
        (DataType::Float, ValueRef::Real(v)) => Ok(Value::F64(v)),
        (DataType::Bytea, ValueRef::Blob(v)) => Ok(Value::Bytea(v.to_vec())),

        (data_type, ValueRef::Text(v)) if physical_type(data_type) == "TEXT" => {
            let text = std::str::from_utf8(v)?;

            if matches!(
                data_type,
                DataType::Map | DataType::List | DataType::Interval | DataType::Point
            ) {
                let value: Value = ron::from_str(text)?;
                if value.get_type().as_ref() != Some(data_type) {
                    return Err(SqliteStorageError::InvalidRonType {
                        expected: data_type.clone(),
                        actual: value.get_type(),
                    });
                }

                Ok(value)
            } else {
                Ok(Value::Str(text.to_owned()).cast(data_type)?)
            }
        }
        _ => Err(SqliteStorageError::InvalidStorageType {
            expected: data_type.clone(),
            actual: value.data_type(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{decode, encode, physical_type},
        crate::SqliteStorageError,
        gluesql_core::{
            ast::DataType,
            data::{Interval, Point, Value},
            error::{Error, ValueError},
        },
        rusqlite::types::{Type, Value as SqlValue, ValueRef},
        std::collections::BTreeMap,
    };

    #[test]
    fn scalar_and_nested_encodings() {
        let cases = vec![
            (Value::Bool(true), SqlValue::Integer(1)),
            (Value::Bool(false), SqlValue::Integer(0)),
            (Value::I8(i8::MIN), SqlValue::Integer(-128)),
            (Value::I16(i16::MIN), SqlValue::Integer(-32768)),
            (Value::I32(i32::MIN), SqlValue::Integer(-2_147_483_648)),
            (Value::I64(i64::MIN), SqlValue::Integer(i64::MIN)),
            (Value::U8(u8::MAX), SqlValue::Integer(255)),
            (Value::U16(u16::MAX), SqlValue::Integer(65535)),
            (Value::U32(u32::MAX), SqlValue::Integer(4_294_967_295)),
            (
                Value::U64(u64::MAX),
                SqlValue::Text("18446744073709551615".to_owned()),
            ),
            (
                Value::I128(i128::MIN),
                SqlValue::Text("-170141183460469231731687303715884105728".to_owned()),
            ),
            (
                Value::U128(u128::MAX),
                SqlValue::Text("340282366920938463463374607431768211455".to_owned()),
            ),
            (Value::F32(1.25), SqlValue::Real(1.25)),
            (Value::F64(f64::INFINITY), SqlValue::Real(f64::INFINITY)),
            (
                Value::Decimal("1.2300".parse().unwrap()),
                SqlValue::Text("1.2300".to_owned()),
            ),
            (
                Value::Str("한글\0'".to_owned()),
                SqlValue::Text("한글\0'".to_owned()),
            ),
            (
                Value::Bytea(vec![0, 128, 255]),
                SqlValue::Blob(vec![0, 128, 255]),
            ),
            (
                Value::Inet("2001:db8::1".parse().unwrap()),
                SqlValue::Text("2001:db8::1".to_owned()),
            ),
            (
                Value::Date("2026-11-11".parse().unwrap()),
                SqlValue::Text("2026-11-11".to_owned()),
            ),
            (
                Value::Time("12:34:56.123456789".parse().unwrap()),
                SqlValue::Text("12:34:56.123456789".to_owned()),
            ),
            (
                Value::Timestamp("2026-11-11T12:34:56.123456789".parse().unwrap()),
                SqlValue::Text("2026-11-11 12:34:56.123456789".to_owned()),
            ),
            (
                Value::Uuid(u128::MAX),
                SqlValue::Text("ffffffff-ffff-ffff-ffff-ffffffffffff".to_owned()),
            ),
            (
                Value::Interval(Interval::Month(i32::MIN)),
                SqlValue::Text("Interval(Month(-2147483648))".to_owned()),
            ),
            (
                Value::Interval(Interval::Microsecond(i64::MIN)),
                SqlValue::Text("Interval(Microsecond(-9223372036854775808))".to_owned()),
            ),
            (
                Value::Point(Point::new(1.25, -0.0)),
                SqlValue::Text("Point((x:1.25,y:-0.0))".to_owned()),
            ),
            (
                Value::List(vec![Value::Null, Value::I16(7)]),
                SqlValue::Text("List([Null,I16(7)])".to_owned()),
            ),
            (
                Value::Map(BTreeMap::from([(
                    "price".to_owned(),
                    Value::Decimal("1.2300".parse().unwrap()),
                )])),
                SqlValue::Text("Map({\"price\":Decimal(\"1.2300\")})".to_owned()),
            ),
        ];

        for (value, expected) in cases {
            let data_type = value.get_type().unwrap();
            assert_eq!(encode(&value, &data_type).unwrap(), expected);

            let restored = decode(ValueRef::from(&expected), &data_type).unwrap();
            assert_eq!(restored, value);
            if let Value::Decimal(decimal) = restored {
                assert_eq!(decimal.scale(), 4);
            }

            assert_eq!(encode(&Value::Null, &data_type).unwrap(), SqlValue::Null);
            assert_eq!(decode(ValueRef::Null, &data_type).unwrap(), Value::Null);

            let expected_type = match expected {
                SqlValue::Integer(_) => "INTEGER",
                SqlValue::Real(_) => "REAL",
                SqlValue::Blob(_) => "BLOB",
                _ => "TEXT",
            };
            assert_eq!(physical_type(&data_type), expected_type);
        }
    }

    #[test]
    fn invalid_integer_ranges_and_storage_classes() {
        for (data_type, value) in [
            (DataType::Int8, 128),
            (DataType::Int16, 32768),
            (DataType::Int32, 2_147_483_648),
            (DataType::Uint8, -1),
            (DataType::Uint16, 65536),
            (DataType::Uint32, 4_294_967_296),
        ] {
            assert!(matches!(
                decode(ValueRef::Integer(value), &data_type),
                Err(SqliteStorageError::IntegerOutOfRange {
                    data_type: actual,
                    value: number,
                }) if actual == data_type && number == value
            ));
        }

        assert!(matches!(
            decode(ValueRef::Integer(2), &DataType::Boolean),
            Err(SqliteStorageError::InvalidBoolean(2))
        ));

        assert!(matches!(
            decode(ValueRef::Text(b"1"), &DataType::Int),
            Err(SqliteStorageError::InvalidStorageType {
                expected: DataType::Int,
                actual: Type::Text
            })
        ));

        assert!(matches!(
            decode(ValueRef::Blob(&[]), &DataType::Text),
            Err(SqliteStorageError::InvalidStorageType {
                expected: DataType::Text,
                actual: Type::Blob
            })
        ));
    }

    #[test]
    fn invalid_text_and_ron_values() {
        assert!(matches!(
            decode(ValueRef::Text(&[255]), &DataType::Text),
            Err(SqliteStorageError::Utf8(_))
        ));

        let error = decode(ValueRef::Text(b"not a decimal"), &DataType::Decimal).unwrap_err();
        let expected = Error::from(ValueError::ConvertFailed {
            value: Value::Str("not a decimal".to_owned()),
            data_type: DataType::Decimal,
        });
        assert!(matches!(error, SqliteStorageError::Glue(actual) if actual == expected));

        assert!(matches!(
            decode(ValueRef::Text(b"{"), &DataType::Map),
            Err(SqliteStorageError::RonDecode(_))
        ));

        assert!(matches!(
            decode(ValueRef::Text(b"List([])"), &DataType::Map),
            Err(SqliteStorageError::InvalidRonType {
                expected: DataType::Map,
                actual: Some(DataType::List)
            })
        ));

        let error = encode(&Value::I64(1), &DataType::Text).unwrap_err();
        let expected = Error::from(ValueError::IncompatibleDataType {
            value: Value::I64(1),
            data_type: DataType::Text,
        });
        assert!(matches!(error, SqliteStorageError::Glue(actual) if actual == expected));

        for value in [Value::F32(f32::NAN), Value::F64(f64::NAN)] {
            assert!(matches!(
                encode(&value, &value.get_type().unwrap()),
                Err(SqliteStorageError::NanNotSupported)
            ));
        }
    }
}
