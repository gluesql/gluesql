use std::collections::HashMap;

use byteorder::{BigEndian, ByteOrder};
use gluesql_core::{
    chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc},
    data::Value,
    prelude::{Error, Result},
};
use num_traits::cast::ToPrimitive;
use parquet::{data_type::ByteArray, record::Field};

#[derive(Debug)]
pub struct ParquetField(pub Field);

impl ParquetField {
    pub fn as_field(&self) -> &Field {
        &self.0
    }
}

impl TryFrom<ParquetField> for Value {
    type Error = Error;

    fn try_from(p: ParquetField) -> Result<Self, Self::Error> {
        match p.as_field() {
            Field::Bool(v) => Ok(Value::Bool(*v)),
            Field::Byte(v) => Ok(Value::I8(*v)),
            Field::Short(v) => Ok(Value::I16(*v)),
            Field::Int(v) => Ok(Value::I32(*v)),
            Field::Long(v) => Ok(Value::I64(*v)),
            Field::UByte(v) => Ok(Value::U8(*v)),
            Field::UShort(v) => Ok(Value::U16(*v)),
            Field::UInt(v) => Ok(Value::U32(*v)),
            Field::ULong(v) => Ok(Value::U64(*v)),
            Field::Float(v) => Ok(Value::F32(*v)),
            Field::Double(v) => Ok(Value::F64(*v)),
            Field::Str(v) => Ok(Value::Str(v.clone())),
            Field::Bytes(v) => Ok(Value::Bytea(v.data().to_vec())),
            Field::Date(v) => {
                let epoch_day = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let epoch_day_and_time =
                    DateTime::<Utc>::from_utc(epoch_day.and_hms_opt(0, 0, 0).unwrap(), Utc);
                Ok(Value::Date(
                    epoch_day_and_time
                        .checked_add_signed(Duration::days(*v as i64))
                        .unwrap()
                        .date_naive(),
                ))
            }
            Field::TimestampMillis(v) => Ok(Value::Timestamp(
                NaiveDateTime::from_timestamp_millis(*v).unwrap(),
            )),
            Field::TimestampMicros(v) => Ok(Value::Timestamp(
                NaiveDateTime::from_timestamp_micros(*v).unwrap(),
            )),
            // To convert parquet's Decimal type to rust_decimal::Decimal, you need to extract the value as i32, i64 or ByteArray and construct a new Decimal based on it.
            // None of the rust_decimal::Decimal constructors take a byte array, so you must use the i128 value. Be careful here, parquet decimal can be 32 bits, 64 bits, or any length, whereas rust_decimal::Decimal only supports up to 128 bits.
            // You also need to consider scale. rust_decimal::Decimal supports scale, which can be used to express decimal precision.
            Field::Decimal(v) => {
                let decimal = match v {
                    parquet::data_type::Decimal::Int32 { value, .. } => {
                        let val = i32::from_be_bytes(*value);
                        Some(Value::Decimal(val.into()))
                    }
                    parquet::data_type::Decimal::Int64 { value, .. } => {
                        let val = i64::from_be_bytes(*value);
                        Some(Value::Decimal(val.into()))
                    }
                    parquet::data_type::Decimal::Bytes { value, .. } => {
                        // The byte array might represent a decimal larger than i64::MAX, so
                        let mut bytes = value.data().to_vec();
                        bytes.resize(16, 0);
                        let val = BigEndian::read_i128(&bytes);
                        Some(Value::Decimal(val.into()))
                    }
                };
                match decimal {
                    Some(v) => Ok(v),
                    None => Err(Error::StorageMsg("Invalid decimal".to_string())),
                }
            }
            Field::Group(v) => {
                let mut map = HashMap::new();
                for (name, field) in v.get_column_iter() {
                    let value: Value = ParquetField(field.clone()).try_into()?;
                    map.insert(name.clone(), value);
                }
                Ok(Value::Map(map))
            }
            Field::ListInternal(v) => {
                let mut list = Vec::new();
                for field in v.elements() {
                    let value: Value = ParquetField(field.clone()).try_into()?;
                    list.push(value);
                }
                Ok(Value::List(list))
            }
            Field::Null => Ok(Value::Null),
            _ => Err(Error::StorageMsg(format!(
                "Unsupported field type: {}",
                std::any::type_name::<&Field>()
            ))),
        }
    }
}

impl TryFrom<Value> for ParquetField {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let field = match value {
            Value::Bool(v) => Field::Bool(v),
            Value::I8(v) => Field::Byte(v),
            Value::I16(v) => Field::Short(v),
            Value::I32(v) => Field::Int(v),
            Value::I64(v) => Field::Long(v),
            Value::U8(v) => Field::UByte(v),
            Value::U16(v) => Field::UShort(v),
            Value::U32(v) => Field::UInt(v),
            Value::U64(v) => Field::ULong(v),
            Value::F32(v) => Field::Float(v),
            Value::F64(v) => Field::Double(v),
            Value::Str(v) => Field::Str(v),
            Value::Bytea(v) => Field::Bytes(v.into()), // Assuming the right conversion here
            Value::Timestamp(v) => {
                // need to decide between TimestampMillis and TimestampMicros
                // Depending on the exact representation in your code, you might need to adjust this conversion
                let millis = v.timestamp_millis();
                Field::TimestampMillis(millis)
            }
            Value::Decimal(v) => {
                let value_as_i128 = v
                    .to_i128()
                    .ok_or_else(|| Error::StorageMsg("Invalid decimal".to_string()))?;
                let bytes = value_as_i128.to_be_bytes();
                let precision = v.scale() as i32; // You need to decide how to set precision
                let scale = v.scale() as i32;
                match bytes.len() {
                    4 => Field::Decimal(parquet::data_type::Decimal::Int32 {
                        value: bytes[0..4].try_into().unwrap(),
                        precision,
                        scale,
                    }),
                    8 => Field::Decimal(parquet::data_type::Decimal::Int64 {
                        value: bytes[0..8].try_into().unwrap(),
                        precision,
                        scale,
                    }),
                    _ => Field::Decimal(parquet::data_type::Decimal::Bytes {
                        value: ByteArray::from(&bytes[..]),
                        precision,
                        scale,
                    }),
                }
            }
            _ => return Err(Error::StorageMsg("Unsupported value type".to_string())),
        };

        Ok(ParquetField(field))
    }
}
