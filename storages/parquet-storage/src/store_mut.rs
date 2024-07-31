use {
    crate::{error::ResultExt, ParquetStorage, ParquetStorageError},
    async_trait::async_trait,
    gluesql_core::{
        ast::{ColumnDef, ToSql},
        chrono::{NaiveDate, Timelike},
        data::{Key, Schema},
        error::Result,
        prelude::{DataType, Error, Value},
        store::{DataRow, StoreMut},
    },
    lazy_static::lazy_static,
    parquet::{
        basic::{ConvertedType, Type},
        column::writer::ColumnWriter,
        data_type::{ByteArray, FixedLenByteArray},
        file::{properties::WriterProperties, writer::SerializedFileWriter},
        format::KeyValue,
        schema::types::Type as SchemaType,
    },
    std::{
        cmp::Ordering,
        collections::HashMap,
        fs::{remove_file, File},
        iter::Peekable,
        sync::Arc,
        vec::IntoIter,
    },
};

lazy_static! {
    static ref GLUESQL_TO_PARQUET_DATA_TYPE_MAPPING: HashMap<DataType, &'static str> = {
        let mut m = HashMap::new();
        m.insert(DataType::Boolean, "Boolean");
        m.insert(DataType::Int8, "Int8");
        m.insert(DataType::Int16, "Int16");
        m.insert(DataType::Int32, "Int32");
        m.insert(DataType::Int, "Int");
        m.insert(DataType::Int128, "Int128");
        m.insert(DataType::Uint8, "Uint8");
        m.insert(DataType::Uint16, "Uint16");
        m.insert(DataType::Uint32, "Uint32");
        m.insert(DataType::Uint64, "Uint64");
        m.insert(DataType::Uint128, "Uint128");
        m.insert(DataType::Float32, "Float32");
        m.insert(DataType::Float, "Float");
        m.insert(DataType::Text, "Text");
        m.insert(DataType::Bytea, "Bytea");
        m.insert(DataType::Inet, "Inet");
        m.insert(DataType::Date, "Date");
        m.insert(DataType::Timestamp, "Timestamp");
        m.insert(DataType::Time, "Time");
        m.insert(DataType::Interval, "Interval");
        m.insert(DataType::Uuid, "Uuid");
        m.insert(DataType::Map, "Map");
        m.insert(DataType::List, "List");
        m.insert(DataType::Decimal, "Decimal");
        m.insert(DataType::Point, "Point");
        m
    };
}

#[async_trait(?Send)]
impl StoreMut for ParquetStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let data_path = self.data_path(schema.table_name.as_str());
        let file = File::create(data_path).map_storage_err()?;
        self.write(schema.clone(), Vec::new(), file)?;

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let schema_path = self.data_path(table_name);
        if schema_path.exists() {
            remove_file(schema_path).map_storage_err()?;
        }
        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        let schema_path = self.data_path(table_name);
        let (prev_rows, schema) = self.scan_data(table_name)?;

        let rows = prev_rows
            .map(|item| Ok(item?.1))
            .chain(rows.into_iter().map(Ok))
            .collect::<Result<Vec<_>>>()?;

        let file = File::create(schema_path).map_storage_err()?;
        self.write(schema, rows, file)
    }

    async fn insert_data(&mut self, table_name: &str, mut rows: Vec<(Key, DataRow)>) -> Result<()> {
        let (prev_rows, schema) = self.scan_data(table_name)?;

        rows.sort_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));

        let sort_merge = SortMerge::new(prev_rows, rows.into_iter());
        let merged = sort_merge.collect::<Result<Vec<_>>>()?;
        self.rewrite(schema, merged)
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let (prev_rows, schema) = self.scan_data(table_name)?;
        let rows = prev_rows
            .filter_map(|result| {
                result
                    .map(|(key, data_row)| {
                        let preservable = !keys.iter().any(|target_key| target_key == &key);

                        preservable.then_some(data_row)
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;

        self.rewrite(schema, rows)
    }
}

struct SortMerge<T: Iterator<Item = Result<(Key, DataRow)>>> {
    left_rows: Peekable<T>,
    right_rows: Peekable<IntoIter<(Key, DataRow)>>,
}

impl<T> SortMerge<T>
where
    T: Iterator<Item = Result<(Key, DataRow)>>,
{
    fn new(left_rows: T, right_rows: IntoIter<(Key, DataRow)>) -> Self {
        let left_rows = left_rows.peekable();
        let right_rows = right_rows.peekable();

        Self {
            left_rows,
            right_rows,
        }
    }
}
impl<T> Iterator for SortMerge<T>
where
    T: Iterator<Item = Result<(Key, DataRow)>>,
{
    type Item = Result<DataRow>;

    fn next(&mut self) -> Option<Self::Item> {
        let left = self.left_rows.peek();
        let right = self.right_rows.peek();

        match (left, right) {
            (Some(Ok((left_key, _))), Some((right_key, _))) => match left_key.cmp(right_key) {
                Ordering::Less => self.left_rows.next(),
                Ordering::Greater => self.right_rows.next().map(Ok),
                Ordering::Equal => {
                    self.left_rows.next();
                    self.right_rows.next().map(Ok)
                }
            }
            .map(|item| Ok(item?.1)),
            (Some(_), _) => self.left_rows.next().map(|item| Ok(item?.1)),
            (None, Some(_)) => self.right_rows.next().map(|item| Ok(item.1)),
            (None, None) => None,
        }
    }
}

impl ParquetStorage {
    fn rewrite(&mut self, schema: Schema, rows: Vec<DataRow>) -> Result<()> {
        let parquet_path = self.data_path(&schema.table_name);
        let file = File::create(parquet_path).map_storage_err()?;
        self.write(schema, rows, file)
    }

    fn write(&mut self, schema: Schema, rows: Vec<DataRow>, file: File) -> Result<()> {
        let schema_type: Arc<SchemaType> =
            self.convert_to_parquet_schema(&schema).map_storage_err()?;

        let metadata = Self::gather_metadata_from_glue_schema(&schema)?;

        let props = Arc::new(
            WriterProperties::builder()
                .set_key_value_metadata(metadata)
                .build(),
        );

        let mut file_writer =
            SerializedFileWriter::new(file, schema_type.clone(), props).map_storage_err()?;

        let mut row_group_writer = file_writer.next_row_group().map_storage_err()?;

        for (i, _) in schema_type.get_fields().iter().enumerate() {
            let mut writer = row_group_writer
                .next_column()
                .map_storage_err()?
                .ok_or(Error::StorageMsg("Expected a column but found None".into()))?;
            let mut col_writer = writer.untyped();
            for row in &rows {
                match row {
                    DataRow::Vec(values) => {
                        let value = values[i].clone();
                        let col_writer = &mut col_writer;
                        match (value, col_writer) {
                            (Value::Null, ColumnWriter::BoolColumnWriter(ref mut typed)) => {
                                typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                            }
                            (Value::Null, ColumnWriter::Int32ColumnWriter(ref mut typed)) => {
                                typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                            }
                            (Value::Null, ColumnWriter::Int64ColumnWriter(ref mut typed)) => {
                                typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                            }
                            (Value::Null, ColumnWriter::Int96ColumnWriter(ref mut typed)) => {
                                typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                            }
                            (Value::Null, ColumnWriter::FloatColumnWriter(ref mut typed)) => {
                                typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                            }
                            (Value::Null, ColumnWriter::DoubleColumnWriter(ref mut typed)) => {
                                typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                            }
                            (Value::Null, ColumnWriter::ByteArrayColumnWriter(ref mut typed)) => {
                                typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                            }
                            (
                                Value::Null,
                                ColumnWriter::FixedLenByteArrayColumnWriter(ref mut typed),
                            ) => {
                                typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                            }
                            (Value::Bool(val), ColumnWriter::BoolColumnWriter(ref mut typed)) => {
                                typed
                                    .write_batch(&[val], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::I8(val), ColumnWriter::Int32ColumnWriter(ref mut typed)) => {
                                typed
                                    .write_batch(&[val as i32], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::I16(val), ColumnWriter::Int32ColumnWriter(ref mut typed)) => {
                                typed
                                    .write_batch(&[val as i32], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::I32(val), ColumnWriter::Int32ColumnWriter(ref mut typed)) => {
                                typed
                                    .write_batch(&[val], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::Date(d), ColumnWriter::Int32ColumnWriter(ref mut typed)) => {
                                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                                    .expect("Invalid epoch date");
                                let days_since_epoch = (d - epoch).num_days() as i32;
                                typed
                                    .write_batch(&[days_since_epoch], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::U8(val), ColumnWriter::Int32ColumnWriter(ref mut typed)) => {
                                typed
                                    .write_batch(&[val as i32], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::U16(val), ColumnWriter::Int32ColumnWriter(ref mut typed)) => {
                                typed
                                    .write_batch(&[val as i32], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::U32(val), ColumnWriter::Int32ColumnWriter(ref mut typed)) => {
                                typed
                                    .write_batch(&[val as i32], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::U64(val), ColumnWriter::Int64ColumnWriter(ref mut typed)) => {
                                typed
                                    .write_batch(&[val as i64], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::I64(val), ColumnWriter::Int64ColumnWriter(ref mut typed)) => {
                                typed
                                    .write_batch(&[val], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::Time(val), ColumnWriter::Int64ColumnWriter(ref mut typed)) => {
                                let total_micros = (val.hour() as i64 * 60 * 60 * 1_000_000) // hours to micros
                                + (val.minute() as i64 * 60 * 1_000_000)                          // minutes to micros
                                + (val.second() as i64 * 1_000_000)                               // seconds to micros
                                + (val.nanosecond() as i64 / 1_000); // nanos to micros
                                typed
                                    .write_batch(&[total_micros], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (
                                Value::Timestamp(val),
                                ColumnWriter::ByteArrayColumnWriter(ref mut typed),
                            ) => {
                                let serialized = bincode::serialize(&val).map_storage_err()?;
                                typed
                                    .write_batch(&[serialized.into()], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (
                                Value::I128(val),
                                ColumnWriter::ByteArrayColumnWriter(ref mut typed),
                            ) => {
                                let serialized = bincode::serialize(&val).map_storage_err()?;
                                typed
                                    .write_batch(&[serialized.into()], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (
                                Value::U128(val),
                                ColumnWriter::ByteArrayColumnWriter(ref mut typed),
                            ) => {
                                let serialized = bincode::serialize(&val).map_storage_err()?;
                                typed
                                    .write_batch(&[serialized.into()], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (
                                Value::Uuid(val),
                                ColumnWriter::FixedLenByteArrayColumnWriter(ref mut typed),
                            ) => {
                                let serialized = bincode::serialize(&val).map_storage_err()?;
                                typed
                                    .write_batch(
                                        &[FixedLenByteArray::from(serialized.to_vec())],
                                        Some(&[1]),
                                        None,
                                    )
                                    .map_storage_err()?;
                            }
                            (Value::F32(val), ColumnWriter::FloatColumnWriter(ref mut typed)) => {
                                typed
                                    .write_batch(&[val], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::F64(val), ColumnWriter::DoubleColumnWriter(ref mut typed)) => {
                                typed
                                    .write_batch(&[val], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (
                                Value::Str(val),
                                ColumnWriter::ByteArrayColumnWriter(ref mut typed),
                            ) => {
                                typed
                                    .write_batch(
                                        &[ByteArray::from(val.as_bytes())],
                                        Some(&[1]),
                                        None,
                                    )
                                    .map_storage_err()?;
                            }
                            (
                                Value::Decimal(val),
                                ColumnWriter::ByteArrayColumnWriter(ref mut typed),
                            ) => {
                                let serialized = bincode::serialize(&val).map_storage_err()?;
                                typed
                                    .write_batch(&[serialized.into()], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (
                                Value::Interval(val),
                                ColumnWriter::ByteArrayColumnWriter(ref mut typed),
                            ) => {
                                let serialized = bincode::serialize(&val).map_storage_err()?;
                                typed
                                    .write_batch(&[serialized.into()], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (
                                Value::Bytea(val),
                                ColumnWriter::ByteArrayColumnWriter(ref mut typed),
                            ) => {
                                let byte_array = ByteArray::from(val);
                                typed
                                    .write_batch(&[byte_array], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::Map(m), ColumnWriter::ByteArrayColumnWriter(typed)) => {
                                let serialized = bincode::serialize(&m).map_storage_err()?;
                                typed
                                    .write_batch(&[serialized.into()], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (
                                Value::List(l),
                                ColumnWriter::ByteArrayColumnWriter(ref mut typed),
                            ) => {
                                let serialized = bincode::serialize(&l).map_storage_err()?;
                                typed
                                    .write_batch(&[serialized.into()], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::Point(p), ColumnWriter::ByteArrayColumnWriter(typed)) => {
                                let serialized = bincode::serialize(&p).map_storage_err()?;
                                typed
                                    .write_batch(&[serialized.into()], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            (Value::Inet(inet), ColumnWriter::ByteArrayColumnWriter(typed)) => {
                                let serialized = bincode::serialize(&inet).map_storage_err()?;
                                typed
                                    .write_batch(&[serialized.into()], Some(&[1]), None)
                                    .map_storage_err()?;
                            }
                            _ => return Err(
                                ParquetStorageError::UnreachableGlueSqlValueTypeForParquetWriter
                                    .into(),
                            ),
                        };
                    }
                    DataRow::Map(map) => {
                        let serialized = bincode::serialize(&map).map_storage_err()?;
                        if let ColumnWriter::ByteArrayColumnWriter(typed) = col_writer {
                            typed
                                .write_batch(&[serialized.into()], Some(&[1]), None)
                                .map_storage_err()?;
                        }
                    }
                }
            }
            writer.close().map_storage_err()?;
        }

        row_group_writer.close().map_storage_err()?;
        file_writer.close().map_storage_err()?;

        Ok(())
    }

    fn convert_to_parquet_schema(
        &self,
        schema: &Schema,
    ) -> Result<Arc<parquet::schema::types::Type>> {
        let mut fields = Vec::new();
        let column_defs = match schema.column_defs {
            Some(ref defs) => defs.clone(),
            None => {
                vec![ColumnDef {
                    name: "schemaless".to_owned(),
                    data_type: DataType::Map,
                    nullable: true,
                    default: None,
                    unique: None,
                    comment: None,
                }]
            }
        };

        for column_def in column_defs {
            let (physical_type, converted_type_option) =
                Self::get_parquet_type_mappings(&column_def.data_type)?;
            let repetition = if column_def.nullable {
                parquet::basic::Repetition::OPTIONAL
            } else {
                parquet::basic::Repetition::REQUIRED
            };

            let mut field_builder = parquet::schema::types::Type::primitive_type_builder(
                column_def.name.as_str(),
                physical_type,
            )
            .with_repetition(repetition)
            .with_length(16); // for FIXED_LEN_BYTE_ARRAY length

            if let Some(converted_type) = converted_type_option {
                field_builder = field_builder.with_converted_type(converted_type);
            }

            let field = field_builder.build().map_storage_err()?;

            fields.push(Arc::new(field));
        }

        let parquet_schema = parquet::schema::types::Type::group_type_builder("schema")
            .with_fields(&mut fields)
            .build()
            .map_storage_err()?;
        Ok(Arc::new(parquet_schema))
    }

    fn gather_metadata_from_glue_schema(schema: &Schema) -> Result<Option<Vec<KeyValue>>> {
        let mut metadata = Vec::new();

        for foreign_key in &schema.foreign_keys {
            metadata.push(KeyValue {
                key: format!("foreign_key_{}", foreign_key.name),
                value: Some(serde_json::to_string(&foreign_key).map_storage_err()?),
            });
        }

        if let Some(column_defs) = &schema.column_defs {
            for column_def in column_defs {
                if let Some(unique_option) = &column_def.unique {
                    let key = format!("unique_option{}", column_def.name);
                    let value = if unique_option.is_primary {
                        Some("primary_key".to_owned())
                    } else {
                        Some("unique".to_owned())
                    };

                    metadata.push(KeyValue { key, value });
                }

                if let Some(default_value) = &column_def.default {
                    metadata.push(KeyValue {
                        key: format!("default_{}", column_def.name),
                        value: Some(ToSql::to_sql(default_value)),
                    });
                }

                if let Some(comment) = &column_def.comment {
                    metadata.push(KeyValue {
                        key: format!("comment_{}", column_def.name),
                        value: Some(comment.to_string()),
                    });
                }

                if let Some(data_type_str) =
                    GLUESQL_TO_PARQUET_DATA_TYPE_MAPPING.get(&column_def.data_type)
                {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some(data_type_str.to_string()),
                    });
                }
            }
            metadata.push(KeyValue {
                key: "schemaless".to_owned(),
                value: Some("false".to_owned()),
            });
        } else {
            metadata.push(KeyValue {
                key: "schemaless".to_owned(),
                value: Some("true".to_owned()),
            });
        }

        if schema.comment.is_some() {
            metadata.push(KeyValue {
                key: "comment".to_owned(),
                value: schema.comment.as_ref().map(ToOwned::to_owned),
            });
        }

        Ok(Some(metadata))
    }

    fn get_parquet_type_mappings(data_type: &DataType) -> Result<(Type, Option<ConvertedType>)> {
        match data_type {
            DataType::Text => Ok((Type::BYTE_ARRAY, Some(ConvertedType::UTF8))),
            DataType::Date => Ok((Type::INT32, Some(ConvertedType::DATE))),
            DataType::Uint8 => Ok((Type::INT32, Some(ConvertedType::UINT_8))),
            DataType::Int => Ok((Type::INT64, Some(ConvertedType::INT_64))),
            DataType::Int8 => Ok((Type::INT32, Some(ConvertedType::INT_8))),
            DataType::Int16 => Ok((Type::INT32, Some(ConvertedType::INT_16))),
            DataType::Int32 => Ok((Type::INT32, Some(ConvertedType::INT_32))),
            DataType::Uint16 => Ok((Type::INT32, Some(ConvertedType::UINT_16))),
            DataType::Uint32 => Ok((Type::INT32, Some(ConvertedType::UINT_32))),
            DataType::Uint64 => Ok((Type::INT64, Some(ConvertedType::UINT_64))),
            DataType::Boolean => Ok((Type::BOOLEAN, None)),
            DataType::Float32 => Ok((Type::FLOAT, None)),
            DataType::Float => Ok((Type::DOUBLE, None)),
            DataType::Uuid => Ok((Type::FIXED_LEN_BYTE_ARRAY, None)),
            DataType::Point => Ok((Type::BYTE_ARRAY, None)),
            DataType::Inet => Ok((Type::BYTE_ARRAY, None)),
            DataType::Uint128 => Ok((Type::BYTE_ARRAY, None)),
            DataType::Int128 => Ok((Type::BYTE_ARRAY, None)),
            DataType::Time => Ok((Type::INT64, None)),
            DataType::Map => Ok((Type::BYTE_ARRAY, None)),
            DataType::List => Ok((Type::BYTE_ARRAY, None)),
            DataType::Interval => Ok((Type::BYTE_ARRAY, None)),
            DataType::Decimal => Ok((Type::BYTE_ARRAY, None)),
            DataType::Timestamp => Ok((Type::BYTE_ARRAY, None)),
            DataType::Bytea => Ok((Type::BYTE_ARRAY, None)),
        }
    }
}
