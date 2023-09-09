use std::collections::HashMap;

use byteorder::{BigEndian, ByteOrder};
use gluesql_core::{
    chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc},
    data::{Schema, Value},
    prelude::{DataType, Error, Result},
};
use parquet::record::Field;

use crate::error::OptionExt;

#[derive(Debug)]
pub struct ParquetField(pub Field);

impl ParquetField {
    pub fn as_field(&self) -> &Field {
        &self.0
    }

    pub fn to_value(&self, schema: &Schema, idx: usize) -> Result<Value> {
        match self.as_field() {
            Field::Bool(v) => Ok(Value::Bool(*v)),
            Field::Byte(v) => Ok(Value::I8(*v)),
            Field::Short(v) => Ok(Value::I16(*v)),
            Field::Int(v) => Ok(Value::I32(*v)),
            Field::Long(v) => {
                if let Some(columns) = &schema.column_defs {
                    if let Some(column) = columns.get(idx) {
                        if column.data_type == DataType::Time {
                            // Convert from microseconds since midnight to NaiveTime
                            let total_seconds = v / 1_000_000;
                            let hours = (total_seconds / 3600) % 24;
                            let minutes = (total_seconds / 60) % 60;
                            let seconds = total_seconds % 60;
                            let micros = v % 1_000_000;

                            let time = NaiveTime::from_hms_micro_opt(
                                hours as u32,
                                minutes as u32,
                                seconds as u32,
                                micros as u32,
                            );

                            match time {
                                Some(t) => return Ok(Value::Time(t)),
                                None => {
                                    return Err(Error::StorageMsg(
                                        "Failed to convert to NaiveTime".to_string(),
                                    ));
                                }
                            }
                        }
                    }
                }
                Ok(Value::I64(*v))
            }
            Field::UByte(v) => Ok(Value::U8(*v)),
            Field::UShort(v) => Ok(Value::U16(*v)),
            Field::UInt(v) => Ok(Value::U32(*v)),
            Field::ULong(v) => Ok(Value::U64(*v)),
            Field::Float(v) => Ok(Value::F32(*v)),
            Field::Double(v) => Ok(Value::F64(*v)),
            Field::Str(v) => Ok(Value::Str(v.clone())),
            Field::Bytes(v) => {
                if let Some(columns) = &schema.column_defs {
                    if let Some(column) = columns.get(idx) {
                        match column.data_type {
                            DataType::Timestamp => {
                                let timestamp = bincode::deserialize(v.data()).map_err(|e| {
                                    Error::StorageMsg(format!(
                                        "Error deserializing timestamp: {:?}",
                                        e
                                    ))
                                })?;
                                return Ok(Value::Timestamp(timestamp));
                            }
                            DataType::Uuid => {
                                let uuid = Self::bytes_to_uuid(v.data())?;
                                return Ok(Value::Uuid(uuid));
                            }
                            DataType::Uint128 => {
                                let uint_val = Self::bytes_to_uint128(v.data())?; // Assuming you have this function defined
                                return Ok(Value::U128(uint_val));
                            }
                            DataType::Int128 => {
                                let int128 = bincode::deserialize(v.data()).map_err(|e| {
                                    Error::StorageMsg(format!(
                                        "Error deserializing int128: {:?}",
                                        e
                                    ))
                                })?;
                                return Ok(Value::I128(int128));
                            }
                            DataType::Interval => {
                                let interval = bincode::deserialize(v.data()).map_err(|e| {
                                    Error::StorageMsg(format!(
                                        "Error deserializing Interval: {:?}",
                                        e
                                    ))
                                })?;
                                return Ok(Value::Interval(interval));
                            }
                            DataType::Decimal => {
                                let decimal = bincode::deserialize(v.data()).map_err(|e| {
                                    Error::StorageMsg(format!(
                                        "Error deserializing Decimal: {:?}",
                                        e
                                    ))
                                })?;
                                return Ok(Value::Decimal(decimal));
                            }
                            DataType::Map => {
                                let map: HashMap<String, Value> = bincode::deserialize(v.data())
                                    .map_err(|e| {
                                        Error::StorageMsg(format!(
                                            "Error deserializing HashMap: {:?}",
                                            e
                                        ))
                                    })?;

                                return Ok(Value::Map(map));
                            }
                            DataType::List => {
                                let list: Vec<Value> =
                                    bincode::deserialize(v.data()).map_err(|e| {
                                        Error::StorageMsg(format!(
                                            "Error deserializing Vec: {:?}",
                                            e
                                        ))
                                    })?;
                                return Ok(Value::List(list));
                            }
                            DataType::Inet => {
                                let inet = bincode::deserialize(v.data()).map_err(|e| {
                                    Error::StorageMsg(format!("Error deserializing inet: {:?}", e))
                                })?;
                                return Ok(Value::Inet(inet));
                            }
                            DataType::Point => {
                                let point = bincode::deserialize(v.data()).map_err(|e| {
                                    Error::StorageMsg(format!("Error deserializing point: {:?}", e))
                                })?;
                                return Ok(Value::Point(point));
                            }
                            _ => {}
                        }
                    }
                }
                Ok(Value::Bytea(v.data().to_vec()))
            }
            Field::Date(v) => {
                let epoch_day = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .ok_or_else(|| Error::StorageMsg("Invalid epoch date".to_string()))?;

                let hour = 0;
                let minute = 0;
                let second = 0;

                let epoch_day_and_time = epoch_day
                    .and_hms_opt(hour, minute, second)
                    .ok_or_else(|| Error::StorageMsg("Invalid time".to_string()))?;

                // Use the recommended method instead of the deprecated one
                let epoch_day_and_time_utc = Utc.from_utc_datetime(&epoch_day_and_time);

                let result_date = epoch_day_and_time_utc
                    .checked_add_signed(Duration::days(*v as i64))
                    .ok_or_else(|| {
                        Error::StorageMsg("Overflow when adding duration to date".to_string())
                    })?;

                Ok(Value::Date(result_date.date_naive()))
            }
            Field::Group(v) => {
                let mut map = HashMap::new();
                for (name, field) in v.get_column_iter() {
                    let value: Value = ParquetField(field.clone()).to_value(schema, idx)?;
                    map.insert(name.clone(), value);
                }
                Ok(Value::Map(map))
            }
            Field::ListInternal(v) => {
                let mut list = Vec::new();
                for field in v.elements() {
                    let value: Value = ParquetField(field.clone()).to_value(schema, idx)?;
                    list.push(value);
                }
                Ok(Value::List(list))
            }
            Field::TimestampMillis(v) => Ok(Value::Timestamp(
                NaiveDateTime::from_timestamp_millis(*v)
                    .map_storage_err("Field::TimestampMillis to Value::Timestamp fail")?,
            )),

            Field::TimestampMicros(v) => Ok(Value::Timestamp(
                NaiveDateTime::from_timestamp_micros(*v)
                    .map_storage_err("Field::TimestampMicros to Value::Timestamp fail")?,
            )),
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
            Field::MapInternal(m) => {
                let mut result_map = HashMap::new();
                for (key_field, value_field) in m.entries() {
                    // Assuming your key is a string (Field::Str)
                    match key_field {
                        Field::Str(key_str) => {
                            // Convert the value_field into a gluesql Value using to_value
                            let glue_value =
                                ParquetField(value_field.clone()).to_value(schema, idx)?;
                            result_map.insert(key_str.clone(), glue_value);
                        }
                        _ => {
                            return Err(Error::StorageMsg(
                                "Expected string key for map".to_string(),
                            ));
                        }
                    }
                }
                Ok(Value::Map(result_map))
            }
            Field::Null => Ok(Value::Null),
        }
    }

    pub fn bytes_to_uuid(bytes: &[u8]) -> Result<u128, Error> {
        if bytes.len() != 16 {
            return Err(Error::StorageMsg(
                "Invalid byte length for u128".to_string(),
            ));
        }
        let mut uuid: u128 = 0;
        for &byte in bytes.iter() {
            uuid = (uuid << 8) | byte as u128;
        }

        Ok(uuid)
    }

    fn bytes_to_uint128(data: &[u8]) -> Result<u128, Error> {
        if data.len() != 16 {
            return Err(Error::StorageMsg(
                "Invalid byte length for u128".to_string(),
            ));
        }

        let mut arr = [0u8; 16];
        arr.clone_from_slice(data);
        Ok(u128::from_le_bytes(arr))
    }
}

// impl TryFrom<ParquetField> for Value {
//     type Error = Error;

//     fn try_from(field: ParquetField) -> Result<Self, Self::Error> {

//     }
// }
// impl TryFrom<Value> for ParquetField {
//     type Error = Error;

//     fn try_from(value: Value) -> Result<Self, Self::Error> {
//         let field = match value {
//             Value::Bool(v) => Field::Bool(v),
//             Value::I8(v) => Field::Byte(v),
//             Value::I16(v) => Field::Short(v),
//             Value::I32(v) => Field::Int(v),
//             Value::I64(v) => Field::Long(v),
//             Value::U8(v) => Field::UByte(v),
//             Value::U16(v) => Field::UShort(v),
//             Value::U32(v) => Field::UInt(v),
//             Value::U64(v) => Field::ULong(v),
//             Value::F32(v) => Field::Float(v),
//             Value::F64(v) => Field::Double(v),
//             Value::Str(v) => Field::Str(v),
//             Value::Bytea(v) => Field::Bytes(v.into()),
//             Value::Uuid(v) => {
//                 let bytes = v.to_be_bytes();
//                 Field::Bytes(ByteArray::from(bytes.to_vec()))
//             }
//             Value::Timestamp(timestamp) => {
//                 let bytes = bincode::serialize(&timestamp).map_err(|e| {
//                     Error::StorageMsg(format!("Error serializing timestamp: {:?}", e))
//                 })?;
//                 Field::Bytes(ByteArray::from(bytes))
//             }
//             Value::Time(v) => {
//                 let total_micros = (v.hour() as i64 * 60 * 60 * 1_000_000) // hours to micros
//                     + (v.minute() as i64 * 60 * 1_000_000)                // minutes to micros
//                     + (v.second() as i64 * 1_000_000)                     // seconds to micros
//                     + (v.nanosecond() as i64 / 1_000); // nanos to micros
//                 Field::Long(total_micros)
//             }
//             Value::Decimal(d) => {
//                 let bytes = bincode::serialize(&d).map_err(|e| {
//                     Error::StorageMsg(format!("Error serializing Decimal: {:?}", e))
//                 })?;
//                 Field::Bytes(ByteArray::from(bytes))
//             }
//             Value::Map(m) => {
//                 let bytes = bincode::serialize(&m).map_err(|e| {
//                     Error::StorageMsg(format!("Error serializing HashMap: {:?}", e))
//                 })?;
//                 Field::Bytes(ByteArray::from(bytes))
//             }
//             Value::List(l) => {
//                 let bytes = bincode::serialize(&l)
//                     .map_err(|e| Error::StorageMsg(format!("Error serializing Vec: {:?}", e)))?;
//                 Field::Bytes(ByteArray::from(bytes))
//             }
//             Value::Date(d) => {
//                 let epoch_day = NaiveDate::from_ymd_opt(1970, 1, 1)
//                     .ok_or_else(|| Error::StorageMsg("Invalid epoch date".to_string()))?;
//                 let days_since_epoch = d.signed_duration_since(epoch_day).num_days();
//                 Field::Date(days_since_epoch as i32)
//             }
//             Value::I128(i) => {
//                 let le_bytes = i.to_le_bytes().to_vec();
//                 Field::Bytes(ByteArray::from(le_bytes))
//             }
//             Value::U128(value) => {
//                 let bytes = value.to_le_bytes();
//                 Field::Bytes(ByteArray::from(&bytes as &[u8]))
//             }

//             Value::Interval(interval) => {
//                 let bytes = bincode::serialize(&interval).map_err(|e| {
//                     Error::StorageMsg(format!("Error serializing Interval: {:?}", e))
//                 })?;
//                 Field::Bytes(ByteArray::from(bytes))
//             }
//             Value::Inet(addr) => {
//                 // Assuming Inet is a simple IPv4 address for this example.
//                 match addr {
//                     IpAddr::V4(v4) => Field::Bytes(ByteArray::from(v4.octets().as_ref())),
//                     IpAddr::V6(v6) => Field::Bytes(ByteArray::from(v6.octets().as_ref())),
//                 }
//             }
//             Value::Point(point) => {
//                 // Assuming Point is a simple (x,y) coordinate for this example.
//                 let mut bytes = Vec::new();
//                 bytes.extend_from_slice(&point.x.to_le_bytes());
//                 bytes.extend_from_slice(&point.y.to_le_bytes());
//                 Field::Bytes(ByteArray::from(bytes))
//             }
//             Value::Null => Field::Null,
//             _ => return Err(Error::StorageMsg("Unsupported value type".to_string())),
//         };

//         Ok(ParquetField(field))
//     }
// }
