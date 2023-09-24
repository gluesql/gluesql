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
                                let uuid = bincode::deserialize(v.data()).map_err(|e| {
                                    Error::StorageMsg(format!("Error deserializing uuid: {:?}", e))
                                })?;
                                return Ok(Value::Uuid(uuid));
                            }
                            DataType::Uint128 => {
                                let uint128 = bincode::deserialize(v.data()).map_err(|e| {
                                    Error::StorageMsg(format!(
                                        "Error deserializing uint128: {:?}",
                                        e
                                    ))
                                })?;
                                return Ok(Value::U128(uint128));
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
                    match key_field {
                        Field::Str(key_str) => {
                            let glue_value =
                                ParquetField(value_field.clone()).to_value(schema, idx)?;
                            result_map.insert(key_str.clone(), glue_value);
                        }
                        _ => {
                            let received_key_type = format!("{:?}", key_field);
                            return Err(Error::StorageMsg(format!(
                                "Unexpected key type for map: received {}, expected String",
                                received_key_type
                            )));
                        }
                    }
                }
                Ok(Value::Map(result_map))
            }
            Field::Null => Ok(Value::Null),
        }
    }
}
