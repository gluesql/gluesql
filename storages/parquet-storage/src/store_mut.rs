use std::sync::Arc;

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

use crate::data_type::ParquetBasicConvertedType;

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
                                let value_bytes = val.to_le_bytes();

                                let byte_array = ByteArray::from(value_bytes.to_vec());

                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(&[byte_array], Some(&[1]), None)
                                        .map_storage_err()?;
                                }
                            }
                            Value::Uuid(val) => {
                                let uuid_bytes = val.to_be_bytes();
                                if let ColumnWriter::FixedLenByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(
                                            &[FixedLenByteArray::from(uuid_bytes.to_vec())],
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
                                let byte_array = bincode::serialize(&val).map_err(|e| {
                                    Error::StorageMsg(format!("Failed to serialize Decimal: {}", e))
                                })?;
                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(&[byte_array.into()], Some(&[1]), None)
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
                                let serialized_map = bincode::serialize(&m).map_err(|e| {
                                    Error::StorageMsg(format!("Failed to serialize HashMap: {}", e))
                                })?;

                                if let ColumnWriter::ByteArrayColumnWriter(typed) = col_writer {
                                    // Write the serialized map
                                    typed
                                        .write_batch(&[serialized_map.into()], Some(&[1]), None)
                                        .map_storage_err()?;
                                } else {
                                    return Err(Error::StorageMsg(
                                        "Expected ByteArrayColumnWriter".into(),
                                    ));
                                }
                            }
                            Value::List(l) => {
                                let byte_array = bincode::serialize(&l).map_err(|e| {
                                    Error::StorageMsg(format!("Failed to serialize list: {}", e))
                                })?;
                                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) =
                                    col_writer
                                {
                                    typed
                                        .write_batch(&[byte_array.into()], Some(&[1]), None)
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
                        let serialized_map = bincode::serialize(&map).map_err(|e| {
                            Error::StorageMsg(format!("Failed to serialize HashMap: {}", e))
                        })?;

                        if let ColumnWriter::ByteArrayColumnWriter(typed) = col_writer {
                            typed
                                .write_batch(&[serialized_map.into()], Some(&[1]), None)
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
        //int 96,
        for column_def in column_defs {
            let converted_type_result =
                ParquetBasicConvertedType::try_from(column_def.data_type.clone());

            let repetition = if column_def.nullable {
                parquet::basic::Repetition::OPTIONAL
            } else {
                parquet::basic::Repetition::REQUIRED
            };

            let (physical_type, converted_type_option) = match converted_type_result {
                Ok(ct) => match ct.as_converted_type() {
                    ConvertedType::UTF8 => (Type::BYTE_ARRAY, Some(ConvertedType::UTF8)),
                    ConvertedType::INT_8 => (Type::INT32, Some(ConvertedType::INT_8)),
                    ConvertedType::INT_16 => (Type::INT32, Some(ConvertedType::INT_16)),
                    ConvertedType::INT_32 => (Type::INT32, Some(ConvertedType::INT_32)),
                    ConvertedType::INT_64 => (Type::INT64, Some(ConvertedType::INT_64)),

                    ConvertedType::DATE => (Type::INT32, Some(ConvertedType::DATE)),
                    ConvertedType::UINT_8 => (Type::INT32, Some(ConvertedType::UINT_8)),
                    ConvertedType::UINT_16 => (Type::INT32, Some(ConvertedType::UINT_16)),
                    ConvertedType::UINT_32 => (Type::INT32, Some(ConvertedType::UINT_32)),
                    ConvertedType::UINT_64 => (Type::INT64, Some(ConvertedType::UINT_64)),
                    ConvertedType::TIME_MICROS => (Type::INT64, Some(ConvertedType::TIME_MICROS)),
                    //ConvertedType::DECIMAL => (Type::INT96, Some(ConvertedType::DECIMAL)),
                    _ => {
                        return Err(Error::StorageMsg(format!(
                            "unsupported data type: {:?}",
                            column_def.data_type
                        )))
                    }
                },
                Err(_) => match column_def.data_type {
                    DataType::Boolean => (Type::BOOLEAN, None),
                    DataType::Float32 => (Type::FLOAT, None),
                    DataType::Float => (Type::DOUBLE, None),
                    DataType::Uuid => (Type::FIXED_LEN_BYTE_ARRAY, None),
                    DataType::Point => (Type::BYTE_ARRAY, None),
                    DataType::Inet => (Type::BYTE_ARRAY, None),
                    DataType::Uint128 => (Type::BYTE_ARRAY, None),
                    DataType::Int128 => (Type::BYTE_ARRAY, None),
                    DataType::Time => (Type::INT64, None),
                    DataType::Map => (Type::BYTE_ARRAY, None),
                    DataType::List => (Type::BYTE_ARRAY, None),
                    DataType::Interval => (Type::BYTE_ARRAY, None),
                    DataType::Decimal => (Type::BYTE_ARRAY, None),
                    DataType::Timestamp => (Type::BYTE_ARRAY, None),
                    DataType::Bytea => (Type::BYTE_ARRAY, None),
                    _ => {
                        return Err(Error::StorageMsg(format!(
                            "unsupported data type: {:?}",
                            column_def.data_type
                        )))
                    }
                },
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

                if let DataType::Uuid = column_def.data_type {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some("Uuid".to_string()),
                    });
                }
                if let DataType::Uint128 = column_def.data_type {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some("Uint128".to_string()),
                    });
                }
                if let DataType::Int128 = column_def.data_type {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some("Int128".to_string()),
                    });
                }
                if let DataType::Time = column_def.data_type {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some("Time".to_string()),
                    });
                }
                if let DataType::Map = column_def.data_type {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some("Map".to_string()),
                    });
                }
                if let DataType::List = column_def.data_type {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some("List".to_string()),
                    });
                }
                if let DataType::Inet = column_def.data_type {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some("Inet".to_string()),
                    });
                }
                if let DataType::Point = column_def.data_type {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some("Point".to_string()),
                    });
                }
                if let DataType::Interval = column_def.data_type {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some("Interval".to_string()),
                    });
                }
                if let DataType::Decimal = column_def.data_type {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some("Decimal".to_string()),
                    });
                }
                if let DataType::Timestamp = column_def.data_type {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some("Timestamp".to_string()),
                    });
                }
            }
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
}
