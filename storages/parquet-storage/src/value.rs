use {
    crate::error::{OptionExt, ParquetStorageError, ResultExt},
    byteorder::{BigEndian, ByteOrder},
    gluesql_core::{
        chrono::{DateTime, Duration, NaiveDate, NaiveTime},
        data::{Schema, Value},
        prelude::{DataType, Error, Result},
    },
    parquet::record::Field,
    std::collections::HashMap,
};

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

                            return NaiveTime::from_hms_micro_opt(
                                hours as u32,
                                minutes as u32,
                                seconds as u32,
                                micros as u32,
                            )
                            .map_storage_err(Error::StorageMsg(
                                "Failed to convert to NaiveTime".to_owned(),
                            ))
                            .map(Value::Time);
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
                                let timestamp = bincode::deserialize(v.data()).map_storage_err()?;
                                return Ok(Value::Timestamp(timestamp));
                            }
                            DataType::Uuid => {
                                let uuid = bincode::deserialize(v.data()).map_storage_err()?;
                                return Ok(Value::Uuid(uuid));
                            }
                            DataType::Uint128 => {
                                let uint128 = bincode::deserialize(v.data()).map_storage_err()?;
                                return Ok(Value::U128(uint128));
                            }
                            DataType::Int128 => {
                                let int128 = bincode::deserialize(v.data()).map_storage_err()?;
                                return Ok(Value::I128(int128));
                            }
                            DataType::Interval => {
                                let interval = bincode::deserialize(v.data()).map_storage_err()?;
                                return Ok(Value::Interval(interval));
                            }
                            DataType::Decimal => {
                                let decimal = bincode::deserialize(v.data()).map_storage_err()?;
                                return Ok(Value::Decimal(decimal));
                            }
                            DataType::Map => {
                                let map: HashMap<String, Value> =
                                    bincode::deserialize(v.data()).map_storage_err()?;
                                return Ok(Value::Map(map));
                            }
                            DataType::List => {
                                let list: Vec<Value> =
                                    bincode::deserialize(v.data()).map_storage_err()?;
                                return Ok(Value::List(list));
                            }
                            DataType::Inet => {
                                let inet = bincode::deserialize(v.data()).map_storage_err()?;
                                return Ok(Value::Inet(inet));
                            }
                            DataType::Point => {
                                let point = bincode::deserialize(v.data()).map_storage_err()?;
                                return Ok(Value::Point(point));
                            }
                            _ => {}
                        }
                    }
                }
                Ok(Value::Bytea(v.data().to_vec()))
            }
            Field::Date(v) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("Invalid epoch date");
                let result_date = epoch + Duration::days(*v as i64);
                Ok(Value::Date(result_date))
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
                DateTime::from_timestamp_millis(*v)
                    .map_storage_err("Field::TimestampMillis to Value::Timestamp fail")?
                    .naive_utc(),
            )),

            Field::TimestampMicros(v) => Ok(Value::Timestamp(
                DateTime::from_timestamp_micros(*v)
                    .map_storage_err("Field::TimestampMicros to Value::Timestamp fail")?
                    .naive_utc(),
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
                    None => Err(Error::StorageMsg("Invalid decimal".to_owned())),
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
                            return Err(ParquetStorageError::UnexpectedKeyTypeForMap(format!(
                                "{:?}",
                                key_field
                            ))
                            .into());
                        }
                    }
                }
                Ok(Value::Map(result_map))
            }
            Field::Null => Ok(Value::Null),
        }
    }
}
