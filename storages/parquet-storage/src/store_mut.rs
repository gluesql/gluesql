use {
    crate::{ParquetStorage, ParquetStorageError, error::ResultExt},
    async_trait::async_trait,
    gluesql_core::{
        ast::{ColumnDef, ToSql},
        chrono::{NaiveDate, Timelike},
        data::{Key, Schema},
        error::Result,
        prelude::{DataType, Error, Value},
        store::{DataRow, StoreMut},
    },
    parquet::{
        basic::{ConvertedType, Type},
        column::writer::{ColumnWriter, ColumnWriterImpl},
        data_type::{
            BoolType, ByteArray, ByteArrayType, DoubleType, FixedLenByteArray,
            FixedLenByteArrayType, FloatType, Int32Type, Int64Type, Int96Type,
        },
        file::{
            properties::WriterProperties,
            writer::{SerializedFileWriter, SerializedRowGroupWriter},
        },
        format::KeyValue,
        schema::types::Type as SchemaType,
    },
    std::{
        cmp::Ordering,
        collections::{BTreeMap, HashMap},
        convert::TryFrom,
        fs::{File, remove_file},
        iter::Peekable,
        sync::{Arc, LazyLock},
        vec::IntoIter,
    },
};

static GLUESQL_TO_PARQUET_DATA_TYPE_MAPPING: LazyLock<HashMap<DataType, &'static str>> =
    LazyLock::new(|| {
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
        m.insert(DataType::FloatVector, "FloatVector");
        m
    });

const DEF_PRESENT: [i16; 1] = [1];
const DEF_NULL: [i16; 1] = [0];

#[async_trait]
impl StoreMut for ParquetStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let data_path = self.data_path(schema.table_name.as_str());
        let file = File::create(data_path).map_storage_err()?;
        Self::write(schema, &[], file)
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
        Self::write(&schema, &rows, file)
    }

    async fn insert_data(&mut self, table_name: &str, mut rows: Vec<(Key, DataRow)>) -> Result<()> {
        let (prev_rows, schema) = self.scan_data(table_name)?;

        rows.sort_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));

        let sort_merge = SortMerge::new(prev_rows, rows.into_iter());
        let merged = sort_merge.collect::<Result<Vec<_>>>()?;
        self.rewrite(&schema, &merged)
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

        self.rewrite(&schema, &rows)
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
    fn rewrite(&mut self, schema: &Schema, rows: &[DataRow]) -> Result<()> {
        let parquet_path = self.data_path(&schema.table_name);
        let file = File::create(parquet_path).map_storage_err()?;
        Self::write(schema, rows, file)
    }

    fn write(schema: &Schema, rows: &[DataRow], file: File) -> Result<()> {
        let schema_type = Self::convert_to_parquet_schema(schema)?;
        let props = Self::build_writer_properties(schema)?;
        let mut file_writer =
            SerializedFileWriter::new(file, Arc::clone(&schema_type), props).map_storage_err()?;

        {
            let mut row_group_writer = file_writer.next_row_group().map_storage_err()?;
            Self::write_row_group(&schema_type, rows, &mut row_group_writer)?;
            row_group_writer.close().map_storage_err()?;
        }

        file_writer.close().map_storage_err()?;
        Ok(())
    }

    fn build_writer_properties(schema: &Schema) -> Result<Arc<WriterProperties>> {
        let metadata = Self::gather_metadata_from_glue_schema(schema)?;

        Ok(Arc::new(
            WriterProperties::builder()
                .set_key_value_metadata(metadata)
                .build(),
        ))
    }

    fn write_row_group(
        schema_type: &SchemaType,
        rows: &[DataRow],
        row_group_writer: &mut SerializedRowGroupWriter<'_, File>,
    ) -> Result<()> {
        for (index, _) in schema_type.get_fields().iter().enumerate() {
            let mut column_writer = row_group_writer
                .next_column()
                .map_storage_err()?
                .ok_or_else(|| Error::StorageMsg("Expected a column but found None".into()))?;

            {
                let untyped = column_writer.untyped();
                Self::write_column(index, rows, untyped)?;
            }

            column_writer.close().map_storage_err()?;
        }

        Ok(())
    }

    fn write_column(
        column_index: usize,
        rows: &[DataRow],
        column_writer: &mut ColumnWriter<'_>,
    ) -> Result<()> {
        for row in rows {
            match row {
                DataRow::Vec(values) => {
                    let value = values.get(column_index).ok_or_else(|| {
                        Error::StorageMsg(format!(
                            "Row length mismatch while writing column {column_index}"
                        ))
                    })?;

                    Self::write_value(value, column_writer)?;
                }
                DataRow::Map(map) => {
                    Self::write_map_row(map, column_writer)?;
                }
            }
        }

        Ok(())
    }

    fn write_map_row(
        map: &BTreeMap<String, Value>,
        column_writer: &mut ColumnWriter<'_>,
    ) -> Result<()> {
        if let ColumnWriter::ByteArrayColumnWriter(writer) = column_writer {
            let serialized = bincode::serialize(map).map_storage_err()?;
            writer
                .write_batch(&[serialized.into()], Some(&DEF_PRESENT), None)
                .map_storage_err()?;

            return Ok(());
        }

        Err(ParquetStorageError::UnreachableGlueSqlValueTypeForParquetWriter.into())
    }

    fn write_value(value: &Value, column_writer: &mut ColumnWriter<'_>) -> Result<()> {
        match column_writer {
            ColumnWriter::BoolColumnWriter(writer) => Self::write_bool_column(writer, value),
            ColumnWriter::Int32ColumnWriter(writer) => Self::write_int32_column(writer, value),
            ColumnWriter::Int64ColumnWriter(writer) => Self::write_int64_column(writer, value),
            ColumnWriter::Int96ColumnWriter(writer) => Self::write_int96_column(writer, value),
            ColumnWriter::FloatColumnWriter(writer) => Self::write_float_column(writer, value),
            ColumnWriter::DoubleColumnWriter(writer) => Self::write_double_column(writer, value),
            ColumnWriter::ByteArrayColumnWriter(writer) => {
                Self::write_byte_array_column(writer, value)
            }
            ColumnWriter::FixedLenByteArrayColumnWriter(writer) => {
                Self::write_fixed_len_byte_array_column(writer, value)
            }
        }
    }

    fn write_bool_column(writer: &mut ColumnWriterImpl<'_, BoolType>, value: &Value) -> Result<()> {
        match value {
            Value::Null => {
                writer
                    .write_batch(&[], Some(&DEF_NULL), None)
                    .map_storage_err()?;
            }
            Value::Bool(val) => {
                writer
                    .write_batch(&[*val], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            _ => {
                return Err(
                    ParquetStorageError::UnreachableGlueSqlValueTypeForParquetWriter.into(),
                );
            }
        }

        Ok(())
    }

    fn write_int32_column(
        writer: &mut ColumnWriterImpl<'_, Int32Type>,
        value: &Value,
    ) -> Result<()> {
        match value {
            Value::Null => {
                writer
                    .write_batch(&[], Some(&DEF_NULL), None)
                    .map_storage_err()?;
            }
            Value::I8(val) => {
                writer
                    .write_batch(&[i32::from(*val)], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            Value::I16(val) => {
                writer
                    .write_batch(&[i32::from(*val)], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            Value::I32(val) => {
                writer
                    .write_batch(&[*val], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            Value::Date(date) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .ok_or_else(|| Error::StorageMsg("Invalid epoch date".to_owned()))?;
                let days = (*date - epoch).num_days();
                let days_since_epoch = i32::try_from(days).map_err(|_| {
                    Error::StorageMsg("Date value exceeds parquet INT32 range".to_owned())
                })?;

                writer
                    .write_batch(&[days_since_epoch], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            Value::U8(val) => {
                writer
                    .write_batch(&[i32::from(*val)], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            Value::U16(val) => {
                writer
                    .write_batch(&[i32::from(*val)], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            Value::U32(val) => {
                let converted = i32::try_from(*val).map_err(|_| {
                    Error::StorageMsg("u32 value exceeds parquet INT32 range".to_owned())
                })?;
                writer
                    .write_batch(&[converted], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            _ => {
                return Err(
                    ParquetStorageError::UnreachableGlueSqlValueTypeForParquetWriter.into(),
                );
            }
        }

        Ok(())
    }

    fn write_int64_column(
        writer: &mut ColumnWriterImpl<'_, Int64Type>,
        value: &Value,
    ) -> Result<()> {
        match value {
            Value::Null => {
                writer
                    .write_batch(&[], Some(&DEF_NULL), None)
                    .map_storage_err()?;
            }
            Value::I64(val) => {
                writer
                    .write_batch(&[*val], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            Value::U64(val) => {
                let converted = i64::try_from(*val).map_err(|_| {
                    Error::StorageMsg("u64 value exceeds parquet INT64 range".to_owned())
                })?;
                writer
                    .write_batch(&[converted], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            Value::Time(time) => {
                let total_micros = i64::from(time.hour()) * 60 * 60 * 1_000_000
                    + i64::from(time.minute()) * 60 * 1_000_000
                    + i64::from(time.second()) * 1_000_000
                    + i64::from(time.nanosecond()) / 1_000;

                writer
                    .write_batch(&[total_micros], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            _ => {
                return Err(
                    ParquetStorageError::UnreachableGlueSqlValueTypeForParquetWriter.into(),
                );
            }
        }

        Ok(())
    }

    fn write_int96_column(
        writer: &mut ColumnWriterImpl<'_, Int96Type>,
        value: &Value,
    ) -> Result<()> {
        match value {
            Value::Null => {
                writer
                    .write_batch(&[], Some(&DEF_NULL), None)
                    .map_storage_err()?;
                Ok(())
            }
            _ => Err(ParquetStorageError::UnreachableGlueSqlValueTypeForParquetWriter.into()),
        }
    }

    fn write_float_column(
        writer: &mut ColumnWriterImpl<'_, FloatType>,
        value: &Value,
    ) -> Result<()> {
        match value {
            Value::Null => {
                writer
                    .write_batch(&[], Some(&DEF_NULL), None)
                    .map_storage_err()?;
            }
            Value::F32(val) => {
                writer
                    .write_batch(&[*val], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            _ => {
                return Err(
                    ParquetStorageError::UnreachableGlueSqlValueTypeForParquetWriter.into(),
                );
            }
        }

        Ok(())
    }

    fn write_double_column(
        writer: &mut ColumnWriterImpl<'_, DoubleType>,
        value: &Value,
    ) -> Result<()> {
        match value {
            Value::Null => {
                writer
                    .write_batch(&[], Some(&DEF_NULL), None)
                    .map_storage_err()?;
            }
            Value::F64(val) => {
                writer
                    .write_batch(&[*val], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            _ => {
                return Err(
                    ParquetStorageError::UnreachableGlueSqlValueTypeForParquetWriter.into(),
                );
            }
        }

        Ok(())
    }

    fn write_byte_array_column(
        writer: &mut ColumnWriterImpl<'_, ByteArrayType>,
        value: &Value,
    ) -> Result<()> {
        match value {
            Value::Null => {
                writer
                    .write_batch(&[], Some(&DEF_NULL), None)
                    .map_storage_err()?;
            }
            Value::Str(val) => {
                writer
                    .write_batch(&[ByteArray::from(val.as_bytes())], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            Value::Timestamp(val) => {
                Self::write_serialized_byte_array(writer, val)?;
            }
            Value::I128(val) => {
                Self::write_serialized_byte_array(writer, val)?;
            }
            Value::U128(val) => {
                Self::write_serialized_byte_array(writer, val)?;
            }
            Value::Decimal(val) => {
                Self::write_serialized_byte_array(writer, val)?;
            }
            Value::Interval(val) => {
                Self::write_serialized_byte_array(writer, val)?;
            }
            Value::Bytea(val) => {
                writer
                    .write_batch(&[ByteArray::from(val.as_slice())], Some(&DEF_PRESENT), None)
                    .map_storage_err()?;
            }
            Value::Map(val) => {
                Self::write_serialized_byte_array(writer, val)?;
            }
            Value::List(val) => {
                Self::write_serialized_byte_array(writer, val)?;
            }
            Value::Point(val) => {
                Self::write_serialized_byte_array(writer, val)?;
            }
            Value::Inet(val) => {
                Self::write_serialized_byte_array(writer, val)?;
            }
            _ => {
                return Err(
                    ParquetStorageError::UnreachableGlueSqlValueTypeForParquetWriter.into(),
                );
            }
        }

        Ok(())
    }

    fn write_serialized_byte_array<T>(
        writer: &mut ColumnWriterImpl<'_, ByteArrayType>,
        value: &T,
    ) -> Result<()>
    where
        T: serde::Serialize,
    {
        let serialized = bincode::serialize(value).map_storage_err()?;
        writer
            .write_batch(&[serialized.into()], Some(&DEF_PRESENT), None)
            .map_storage_err()?;
        Ok(())
    }

    fn write_fixed_len_byte_array_column(
        writer: &mut ColumnWriterImpl<'_, FixedLenByteArrayType>,
        value: &Value,
    ) -> Result<()> {
        match value {
            Value::Null => {
                writer
                    .write_batch(&[], Some(&DEF_NULL), None)
                    .map_storage_err()?;
            }
            Value::Uuid(val) => {
                let serialized = bincode::serialize(val).map_storage_err()?;
                writer
                    .write_batch(
                        &[FixedLenByteArray::from(serialized)],
                        Some(&DEF_PRESENT),
                        None,
                    )
                    .map_storage_err()?;
            }
            _ => {
                return Err(
                    ParquetStorageError::UnreachableGlueSqlValueTypeForParquetWriter.into(),
                );
            }
        }

        Ok(())
    }

    fn convert_to_parquet_schema(schema: &Schema) -> Result<Arc<parquet::schema::types::Type>> {
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
                Self::get_parquet_type_mappings(&column_def.data_type);
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
                        value: Some(comment.clone()),
                    });
                }

                if let Some(data_type_str) =
                    GLUESQL_TO_PARQUET_DATA_TYPE_MAPPING.get(&column_def.data_type)
                {
                    metadata.push(KeyValue {
                        key: format!("data_type{}", column_def.name),
                        value: Some((*data_type_str).to_owned()),
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

    fn get_parquet_type_mappings(data_type: &DataType) -> (Type, Option<ConvertedType>) {
        match data_type {
            DataType::Text => (Type::BYTE_ARRAY, Some(ConvertedType::UTF8)),
            DataType::Date => (Type::INT32, Some(ConvertedType::DATE)),
            DataType::Uint8 => (Type::INT32, Some(ConvertedType::UINT_8)),
            DataType::Int => (Type::INT64, Some(ConvertedType::INT_64)),
            DataType::Int8 => (Type::INT32, Some(ConvertedType::INT_8)),
            DataType::Int16 => (Type::INT32, Some(ConvertedType::INT_16)),
            DataType::Int32 => (Type::INT32, Some(ConvertedType::INT_32)),
            DataType::Uint16 => (Type::INT32, Some(ConvertedType::UINT_16)),
            DataType::Uint32 => (Type::INT32, Some(ConvertedType::UINT_32)),
            DataType::Uint64 => (Type::INT64, Some(ConvertedType::UINT_64)),
            DataType::Boolean => (Type::BOOLEAN, None),
            DataType::Float32 => (Type::FLOAT, None),
            DataType::Float => (Type::DOUBLE, None),
            DataType::Uuid => (Type::FIXED_LEN_BYTE_ARRAY, None),
            DataType::Time => (Type::INT64, None),
            DataType::Point
            | DataType::Inet
            | DataType::Uint128
            | DataType::Int128
            | DataType::Map
            | DataType::List
            | DataType::Interval
            | DataType::Decimal
            | DataType::Timestamp
            | DataType::Bytea
            | DataType::FloatVector => (Type::BYTE_ARRAY, None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs::{File, remove_file},
        path::PathBuf,
    };

    use gluesql_core::chrono::NaiveTime;
    use gluesql_core::{ast::ColumnDef, data::Schema, store::DataRow};
    use parquet::basic::Repetition;
    use uuid::Uuid;

    use super::*;

    fn temp_file(name: &str) -> (PathBuf, File) {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "parquet-storage-{name}-{}-{}.parquet",
            std::process::id(),
            Uuid::new_v4()
        ));
        let file = File::create(&path).expect("create temp parquet file");
        (path, file)
    }

    fn build_schema(columns: Vec<(&str, DataType, bool)>) -> Schema {
        Schema {
            table_name: "coverage".into(),
            column_defs: Some(
                columns
                    .into_iter()
                    .map(|(name, data_type, nullable)| ColumnDef {
                        name: name.into(),
                        data_type,
                        nullable,
                        default: None,
                        unique: None,
                        comment: None,
                    })
                    .collect(),
            ),
            indexes: vec![],
            engine: None,
            foreign_keys: vec![],
            comment: None,
        }
    }

    #[test]
    fn write_accepts_nulls_and_converted_values() {
        let schema = build_schema(vec![
            ("flag", DataType::Boolean, true),
            ("int_col", DataType::Int, true),
            ("uint_col", DataType::Uint64, false),
            ("time_col", DataType::Time, false),
            ("bytea_col", DataType::Bytea, true),
            ("uuid_col", DataType::Uuid, true),
        ]);

        let time = NaiveTime::from_hms_opt(1, 2, 3).expect("valid time");
        let other_time = NaiveTime::from_hms_opt(10, 20, 30).expect("valid time");

        let rows = vec![
            DataRow::Vec(vec![
                Value::Null,
                Value::Null,
                Value::U64(42),
                Value::Time(time),
                Value::Null,
                Value::Null,
            ]),
            DataRow::Vec(vec![
                Value::Bool(true),
                Value::I64(-99),
                Value::U64(123),
                Value::Time(other_time),
                Value::Bytea(vec![1, 2, 3, 4]),
                Value::Uuid(Uuid::new_v4().as_u128()),
            ]),
        ];

        let (path, file) = temp_file("write-ok");
        ParquetStorage::write(&schema, &rows, file).expect("write should succeed");
        remove_file(path).ok();
    }

    #[test]
    fn write_fails_on_row_length_mismatch() {
        let schema = build_schema(vec![
            ("flag", DataType::Boolean, true),
            ("int_col", DataType::Int, true),
        ]);

        let rows = vec![DataRow::Vec(vec![Value::Bool(true)])];
        let (path, file) = temp_file("row-len-mismatch");
        let err = ParquetStorage::write(&schema, &rows, file).expect_err("should fail");
        assert!(format!("{err}").contains("Row length mismatch"));
        remove_file(path).ok();
    }

    #[test]
    fn write_fails_for_map_rows_with_non_byte_array_columns() {
        let schema = build_schema(vec![("flag", DataType::Boolean, false)]);
        let map_row = DataRow::Map(BTreeMap::from([(String::from("key"), Value::Bool(true))]));
        let (path, file) = temp_file("map-row");
        let err = ParquetStorage::write(&schema, &[map_row], file).expect_err("should fail");
        assert!(format!("{err}").contains("Unreachable"));
        remove_file(path).ok();
    }

    #[test]
    fn write_int32_column_errors_on_out_of_range_values() {
        let schema = build_schema(vec![("small", DataType::Int32, false)]);
        let rows = vec![DataRow::Vec(vec![Value::U32(u32::MAX)])];
        let (path, file) = temp_file("int32-out-of-range");
        let err = ParquetStorage::write(&schema, &rows, file).expect_err("should fail");
        assert!(format!("{err}").contains("u32 value exceeds parquet INT32 range"));
        remove_file(path).ok();
    }

    #[test]
    fn write_int32_column_errors_on_invalid_types() {
        let schema = build_schema(vec![("small", DataType::Int32, false)]);
        let rows = vec![DataRow::Vec(vec![Value::Str("oops".into())])];
        let (path, file) = temp_file("int32-invalid-type");
        let err = ParquetStorage::write(&schema, &rows, file).expect_err("should fail");
        assert!(format!("{err}").contains("Unreachable"));
        remove_file(path).ok();
    }

    #[test]
    fn write_int64_column_errors_on_out_of_range_values() {
        let schema = build_schema(vec![("big", DataType::Int, false)]);
        let rows = vec![DataRow::Vec(vec![Value::U64(u64::MAX)])];
        let (path, file) = temp_file("int64-out-of-range");
        let err = ParquetStorage::write(&schema, &rows, file).expect_err("should fail");
        assert!(format!("{err}").contains("u64 value exceeds parquet INT64 range"));
        remove_file(path).ok();
    }

    #[test]
    fn write_bool_column_errors_on_invalid_types() {
        let schema = build_schema(vec![("flag", DataType::Boolean, false)]);
        let rows = vec![DataRow::Vec(vec![Value::Str("not-bool".into())])];
        let (path, file) = temp_file("bool-invalid-type");
        let err = ParquetStorage::write(&schema, &rows, file).expect_err("should fail");
        assert!(format!("{err}").contains("Unreachable"));
        remove_file(path).ok();
    }

    #[test]
    fn write_byte_array_column_errors_on_invalid_types() {
        let schema = build_schema(vec![("bytes", DataType::Bytea, false)]);
        let rows = vec![DataRow::Vec(vec![Value::Bool(true)])];
        let (path, file) = temp_file("bytearray-invalid-type");
        let err = ParquetStorage::write(&schema, &rows, file).expect_err("should fail");
        assert!(format!("{err}").contains("Unreachable"));
        remove_file(path).ok();
    }

    #[test]
    fn write_fixed_len_byte_array_column_errors_on_invalid_types() {
        let schema = build_schema(vec![("id", DataType::Uuid, false)]);
        let rows = vec![DataRow::Vec(vec![Value::Str("not-uuid".into())])];
        let (path, file) = temp_file("fixed-len-invalid-type");
        let err = ParquetStorage::write(&schema, &rows, file).expect_err("should fail");
        assert!(format!("{err}").contains("Unreachable"));
        remove_file(path).ok();
    }

    #[test]
    fn write_float_column_errors_on_invalid_types() {
        let schema = build_schema(vec![("float_col", DataType::Float32, false)]);
        let rows = vec![DataRow::Vec(vec![Value::Bool(true)])];
        let (path, file) = temp_file("float-invalid-type");
        let err = ParquetStorage::write(&schema, &rows, file).expect_err("should fail");
        assert!(format!("{err}").contains("Unreachable"));
        remove_file(path).ok();
    }

    #[test]
    fn write_double_column_errors_on_invalid_types() {
        let schema = build_schema(vec![("double_col", DataType::Float, false)]);
        let rows = vec![DataRow::Vec(vec![Value::Bool(false)])];
        let (path, file) = temp_file("double-invalid-type");
        let err = ParquetStorage::write(&schema, &rows, file).expect_err("should fail");
        assert!(format!("{err}").contains("Unreachable"));
        remove_file(path).ok();
    }

    #[test]
    fn write_row_group_handles_multiple_columns() {
        let schema = build_schema(vec![
            ("col_a", DataType::Int32, false),
            ("col_b", DataType::Bytea, false),
        ]);
        let rows = vec![DataRow::Vec(vec![
            Value::I32(1),
            Value::Bytea(vec![1, 2, 3]),
        ])];
        let (path, file) = temp_file("row-group-multi-column");
        ParquetStorage::write(&schema, &rows, file).expect("write should succeed");
        remove_file(path).ok();
    }

    fn build_int96_schema_type() -> Arc<SchemaType> {
        let field = SchemaType::primitive_type_builder("int96_col", Type::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("build int96 column");
        SchemaType::group_type_builder("schema")
            .with_fields(&mut vec![Arc::new(field)])
            .build()
            .map(Arc::new)
            .expect("build schema type")
    }

    fn write_with_schema_type(schema_type: &Arc<SchemaType>, rows: &[DataRow]) -> Result<()> {
        let (path, file) = temp_file("custom-schema");
        let props = Arc::new(WriterProperties::builder().build());
        let mut file_writer =
            SerializedFileWriter::new(file, Arc::clone(schema_type), props).expect("writer");
        {
            let mut row_group_writer = file_writer.next_row_group().expect("row group");
            ParquetStorage::write_row_group(schema_type, rows, &mut row_group_writer)?;
            row_group_writer.close().map_storage_err()?;
        }
        file_writer.close().map_storage_err()?;
        remove_file(path).ok();
        Ok(())
    }

    #[test]
    fn write_int96_column_handles_nulls() {
        let schema_type = build_int96_schema_type();
        let rows = vec![DataRow::Vec(vec![Value::Null])];
        write_with_schema_type(&schema_type, &rows).expect("int96 null write");
    }

    #[test]
    fn write_int96_column_rejects_non_null_values() {
        let schema_type = build_int96_schema_type();
        let rows = vec![DataRow::Vec(vec![Value::Bool(true)])];
        let err = write_with_schema_type(&schema_type, &rows).expect_err("should fail");
        assert!(format!("{err}").contains("Unreachable"));
    }
}
