use std::{collections::HashMap, sync::Arc};

use gluesql_core::{
    ast::{ColumnDef, ToSql},
    chrono::{NaiveDate, Timelike},
    prelude::{DataType, Error, Value},
};
use parquet::{
    basic::{ConvertedType, Type},
    column::writer::ColumnWriter,
    data_type::{ByteArray, FixedLenByteArray},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    format::KeyValue,
    schema::types::Type as SchemaType,
};

use lazy_static::lazy_static;
use {
    crate::{error::ResultExt, ParquetStorage},
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, StoreMut},
    },
    std::{
        fs::{remove_file, File},
        {cmp::Ordering, iter::Peekable, vec::IntoIter},
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

        if let true = schema_path.exists() {
            remove_file(schema_path).map_storage_err()?
        }

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

        let metadata = Self::gather_metadata_from_glue_schema(&schema);

        // Set metadata using the WriterPropertiesBuilder
        let props = Arc::new(
            WriterProperties::builder()
                .set_key_value_metadata(metadata)
                .build(),
        );

        let mut file_writer =
            SerializedFileWriter::new(file, schema_type.clone(), props).map_err(|e| {
                Error::StorageMsg(format!("Failed to create SerializedFileWriter: {}", e))
            })?;

        let mut row_group_writer = file_writer.next_row_group().map_storage_err()?;

        for (i, _) in schema_type.get_fields().iter().enumerate() {
            let mut writer = row_group_writer
                .next_column()
                .map_storage_err()?
                .ok_or(Error::StorageMsg("Expected a column but found None".into()))?;
            let col_writer = writer.untyped();
            for row in &rows {
                match row {
                    DataRow::Vec(values) => {
                        if values.len() != schema_type.get_fields().len() {
                            return Err(Error::StorageMsg(format!(
                                "Mismatch between schema fields and DataRow values. Expected {} fields but got {} values.",
                                schema_type.get_fields().len(),
                                values.len()
                            )));
                        }
                        let value = values[i].clone();

                        match value {
                            Value::Null => {
                                if let ColumnWriter::BoolColumnWriter(ref mut typed) = col_writer {
                                    typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                                }
                                if let ColumnWriter::Int32ColumnWriter(ref mut typed) = col_writer {
                                    typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                                }
                                if let ColumnWriter::Int64ColumnWriter(ref mut typed) = col_writer {
                                    typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                                }
                                if let ColumnWriter::Int96ColumnWriter(ref mut typed) = col_writer {
                                    typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                                }
                                if let ColumnWriter::FloatColumnWriter(ref mut typed) = col_writer {
                                    typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                                }
                                if let ColumnWriter::DoubleColumnWriter(ref mut typed) = col_writer
                                {
                                    typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                                }
                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                                }
                                if let ColumnWriter::FixedLenByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed.write_batch(&[], Some(&[0]), None).map_storage_err()?;
                                }
                            }
                            Value::Bool(val) => {
                                // Handle boolean value case.
                                if let ColumnWriter::BoolColumnWriter(ref mut typed) = col_writer {
                                    typed
                                        .write_batch(&[val], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::I8(val) => {
                                if let ColumnWriter::Int32ColumnWriter(ref mut typed) = col_writer {
                                    typed
                                        .write_batch(&[val as i32], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::I16(val) => {
                                if let ColumnWriter::Int32ColumnWriter(ref mut typed) = col_writer {
                                    typed
                                        .write_batch(&[val as i32], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::I32(val) => {
                                if let ColumnWriter::Int32ColumnWriter(ref mut typed) = col_writer {
                                    typed
                                        .write_batch(&[val], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::Date(d) => {
                                if let ColumnWriter::Int32ColumnWriter(ref mut typed) = col_writer {
                                    let epoch =
                                        NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| {
                                            Error::StorageMsg(
                                                "Invalid epoch data while write function"
                                                    .to_string(),
                                            )
                                        })?;
                                    let days_since_epoch = (d - epoch).num_days() as i32;
                                    typed
                                        .write_batch(&[days_since_epoch], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::U8(val) => {
                                if let ColumnWriter::Int32ColumnWriter(ref mut typed) = col_writer {
                                    typed
                                        .write_batch(&[val as i32], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::U16(val) => {
                                // Handle integer value cases. Here, just showing Int32ColumnWriter.
                                if let ColumnWriter::Int32ColumnWriter(ref mut typed) = col_writer {
                                    typed
                                        .write_batch(&[val as i32], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::U32(val) => {
                                if let ColumnWriter::Int32ColumnWriter(ref mut typed) = col_writer {
                                    typed
                                        .write_batch(&[val as i32], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::U64(val) => {
                                if let ColumnWriter::Int64ColumnWriter(ref mut typed) = col_writer {
                                    typed
                                        .write_batch(&[val as i64], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::I64(val) => {
                                if let ColumnWriter::Int64ColumnWriter(ref mut typed) = col_writer {
                                    typed
                                        .write_batch(&[val], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::Time(val) => {
                                let total_micros = (val.hour() as i64 * 60 * 60 * 1_000_000) // hours to micros
                                + (val.minute() as i64 * 60 * 1_000_000)                // minutes to micros
                                + (val.second() as i64 * 1_000_000)                     // seconds to micros
                                + (val.nanosecond() as i64 / 1_000); // nanos to micros
                                if let ColumnWriter::Int64ColumnWriter(ref mut typed) = col_writer {
                                    typed
                                        .write_batch(&[total_micros], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::Timestamp(val) => {
                                let serialized = bincode::serialize(&val).map_err(|e| {
                                    Error::StorageMsg(format!(
                                        "Failed to serialize Interval: {}",
                                        e
                                    ))
                                })?;
                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(&[serialized.into()], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::I128(val) => {
                                let serialized = bincode::serialize(&val).map_err(|e| {
                                    Error::StorageMsg(format!("Failed to serialize I128: {}", e))
                                })?;
                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(&[serialized.into()], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::U128(val) => {
                                let serialized = bincode::serialize(&val).map_err(|e| {
                                    Error::StorageMsg(format!("Failed to serialize I128: {}", e))
                                })?;
                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(&[serialized.into()], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::Uuid(val) => {
                                let serialized = bincode::serialize(&val).map_err(|e| {
                                    Error::StorageMsg(format!("Failed to serialize I128: {}", e))
                                })?;
                                if let ColumnWriter::FixedLenByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(
                                            &[FixedLenByteArray::from(serialized.to_vec())],
                                            Some(&[1]),
                                            None,
                                        )
                                        .map_storage_err()?;
                                }
                            }
                            Value::F32(val) => {
                                if let ColumnWriter::FloatColumnWriter(ref mut typed) = col_writer {
                                    typed
                                        .write_batch(&[val], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::F64(val) => {
                                if let ColumnWriter::DoubleColumnWriter(ref mut typed) = col_writer
                                {
                                    typed
                                        .write_batch(&[val], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::Str(val) => {
                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(
                                            &[ByteArray::from(val.as_bytes())],
                                            Some(&[1]),
                                            None,
                                        )
                                        .map_storage_err()?;
                                }
                            }
                            Value::Decimal(val) => {
                                // Convert the decimal value to a fixed-length byte array.
                                let serialized = bincode::serialize(&val).map_err(|e| {
                                    Error::StorageMsg(format!("Failed to serialize Decimal: {}", e))
                                })?;
                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(&[serialized.into()], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::Interval(val) => {
                                let serialized = bincode::serialize(&val).map_err(|e| {
                                    Error::StorageMsg(format!(
                                        "Failed to serialize Interval: {}",
                                        e
                                    ))
                                })?;
                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(&[serialized.into()], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::Bytea(val) => {
                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    let byte_array = ByteArray::from(val);
                                    typed
                                        .write_batch(&[byte_array], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::Map(m) => {
                                // Serialize the entire HashMap to a byte vector
                                let serialized = bincode::serialize(&m).map_err(|e| {
                                    Error::StorageMsg(format!("Failed to serialize HashMap: {}", e))
                                })?;

                                if let ColumnWriter::ByteArrayColumnWriter(typed) = col_writer {
                                    // Write the serialized map
                                    typed
                                        .write_batch(&[serialized.into()], Some(&[1]), None)
                                        .map_storage_err()?;
                                } else {
                                    return Err(Error::StorageMsg(
                                        "Expected ByteArrayColumnWriter".into(),
                                    ));
                                }
                            }
                            Value::List(l) => {
                                let serialized = bincode::serialize(&l).map_err(|e| {
                                    Error::StorageMsg(format!("Failed to serialize list: {}", e))
                                })?;
                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(&[serialized.into()], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::Point(p) => {
                                let serialized = bincode::serialize(&p).map_err(|e| {
                                    Error::StorageMsg(format!("Failed to serialize HashMap: {}", e))
                                })?;
                                if let ColumnWriter::ByteArrayColumnWriter(typed) = col_writer {
                                    // Write the serialized map
                                    typed
                                        .write_batch(&[serialized.into()], Some(&[1]), None)
                                        .map_storage_err()?;
                                } else {
                                    return Err(Error::StorageMsg(
                                        "Expected ByteArrayColumnWriter".into(),
                                    ));
                                }
                            }
                            Value::Inet(inet) => {
                                let serialized = bincode::serialize(&inet).map_err(|e| {
                                    Error::StorageMsg(format!("Failed to serialize HashMap: {}", e))
                                })?;
                                if let ColumnWriter::ByteArrayColumnWriter(typed) = col_writer {
                                    // Write the serialized map
                                    typed
                                        .write_batch(&[serialized.into()], Some(&[1]), None)
                                        .map_storage_err()?;
                                } else {
                                    return Err(Error::StorageMsg(
                                        "Expected ByteArrayColumnWriter".into(),
                                    ));
                                }
                            }
                        }
                    }
                    DataRow::Map(map) => {
                        let serialized = bincode::serialize(&map).map_err(|e| {
                            Error::StorageMsg(format!("Failed to serialize HashMap: {}", e))
                        })?;

                        if let ColumnWriter::ByteArrayColumnWriter(typed) = col_writer {
                            typed
                                .write_batch(&[serialized.into()], Some(&[1]), None)
                                .map_storage_err()?;
                        } else {
                            return Err(Error::StorageMsg("Expected ByteArrayColumnWriter".into()));
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
                    name: "schemaless".to_string(),
                    data_type: DataType::Map,
                    nullable: true,
                    default: None,
                    unique: None,
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
            .with_scale(0) // Set the scale for Uint128
            .with_length(16);

            if let Some(converted_type) = converted_type_option {
                field_builder = field_builder.with_converted_type(converted_type);
            }

            let field = field_builder
                .build()
                .map_err(|e| Error::StorageMsg(format!("Failed to create schema type: {}", e)))?;

            fields.push(Arc::new(field));
        }

        let parquet_schema = parquet::schema::types::Type::group_type_builder("schema")
            .with_fields(&mut fields)
            .build()
            .map_storage_err()?;
        Ok(Arc::new(parquet_schema))
    }

    fn gather_metadata_from_glue_schema(schema: &Schema) -> Option<Vec<KeyValue>> {
        let mut metadata = Vec::new();

        if let Some(column_defs) = &schema.column_defs {
            for column_def in column_defs {
                if let Some(unique_option) = &column_def.unique {
                    let key = format!("unique_option{}", column_def.name);
                    let value = if unique_option.is_primary {
                        Some("primary_key".to_string())
                    } else {
                        Some("unique".to_string())
                    };

                    metadata.push(KeyValue { key, value });
                }

                if let Some(default_value) = &column_def.default {
                    metadata.push(KeyValue {
                        key: format!("default_{}", column_def.name),
                        value: Some(ToSql::to_sql(default_value)),
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
                key: "schemaless".to_string(),
                value: Some("false".to_string()),
            });
        } else {
            metadata.push(KeyValue {
                key: "schemaless".to_string(),
                value: Some("true".to_string()),
            });
        }

        if metadata.is_empty() {
            None
        } else {
            Some(metadata)
        }
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
