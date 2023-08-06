use std::{io::Write, sync::Arc};

use gluesql_core::prelude::Error;
use parquet::{
    column::writer::ColumnWriter,
    data_type::{ByteArray, Int96},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::Type as SchemaType,
};

use crate::{data_type::ParquetBasicPhysicalType, error::OptionExt};

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
        File::create(data_path).map_storage_err()?;

        if schema.column_defs.is_some() {
            let schema_path = self.data_path(schema.table_name.as_str());
            let ddl = schema.to_ddl();
            let mut file = File::create(schema_path).map_storage_err()?;

            file.write_all(ddl.as_bytes()).map_storage_err()?;
        }

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
        println!("merged complete ! {:?}", merged);
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
        println!("rewrite files : {:?}", file);
        self.write(schema, rows, file)
    }

    fn write(&mut self, schema: Schema, rows: Vec<DataRow>, file: File) -> Result<()> {
        let schema_type: Arc<SchemaType> = self.convert_schema(&schema).map_storage_err()?;
        let props = Arc::new(WriterProperties::builder().build());

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
                        let value = &values[i];
                        println!("DataRow row's cur value {:?}", value);

                        match col_writer {
                            ColumnWriter::Int32ColumnWriter(ref mut typed) => {
                                let value: i32 = value.try_into()?;
                                typed.write_batch(&[value], None, None).map_storage_err()?;
                            }
                            ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                                let value: i64 = value.try_into()?;
                                typed.write_batch(&[value], None, None).map_storage_err()?;
                            }
                            ColumnWriter::Int96ColumnWriter(ref mut typed) => {
                                let value: i128 = value.try_into()?;
                                let value_bytes = value.to_le_bytes();
                                let elem0 = u32::from_le_bytes([
                                    value_bytes[0],
                                    value_bytes[1],
                                    value_bytes[2],
                                    value_bytes[3],
                                ]);
                                let elem1 = u32::from_le_bytes([
                                    value_bytes[4],
                                    value_bytes[5],
                                    value_bytes[6],
                                    value_bytes[7],
                                ]);
                                let elem2 = u32::from_le_bytes([
                                    value_bytes[8],
                                    value_bytes[9],
                                    value_bytes[10],
                                    value_bytes[11],
                                ]);

                                let mut int96 = Int96::new();
                                int96.set_data(elem0, elem1, elem2);
                                typed.write_batch(&[int96], None, None).map_storage_err()?;
                            }
                            ColumnWriter::FloatColumnWriter(ref mut typed) => {
                                let value: f32 = value.try_into()?;
                                typed.write_batch(&[value], None, None).map_storage_err()?;
                            }
                            ColumnWriter::DoubleColumnWriter(ref mut typed) => {
                                let value: f64 = value.try_into()?;
                                typed.write_batch(&[value], None, None).map_storage_err()?;
                            }
                            ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                                let value: String = value.into();
                                let byte_array = ByteArray::from(value.as_bytes());
                                typed
                                    .write_batch(&[byte_array], None, None)
                                    .map_storage_err()?;
                            }
                            _ => return Err(Error::StorageMsg("Unexpected data type".into())),
                        }
                    }
                    _ => return Err(Error::StorageMsg("Unexpected row format".into())),
                }
            }
            writer.close().map_storage_err()?;
        }

        row_group_writer.close().map_storage_err()?;
        file_writer.close().map_storage_err()?;

        Ok(())
    }

    fn convert_schema(&self, schema: &Schema) -> Result<Arc<parquet::schema::types::Type>> {
        let mut fields = Vec::new();

        if let Some(column_defs) = &schema.column_defs {
            for column_def in column_defs {
                let physical_type_result =
                    ParquetBasicPhysicalType::try_from(column_def.data_type.clone());

                // Here we handle the potential error from the conversion
                let physical_type = match physical_type_result {
                    Ok(pt) => *pt.as_physical_type(),
                    Err(_) => return Err(Error::StorageMsg("no matching physical type".into())), // replace "Error message" with a more descriptive message
                };

                // Assuming column_def.name is the name of the field
                let field = parquet::schema::types::Type::primitive_type_builder(
                    column_def.name.as_str(),
                    physical_type,
                )
                .with_repetition(parquet::basic::Repetition::REQUIRED) // or OPTIONAL or REPEATED
                .build()
                .map_err(|e| Error::StorageMsg(format!("Failed to create schema type: {}", e)))?;

                fields.push(Arc::new(field));
            }
        }

        Ok(Arc::new(
            parquet::schema::types::Type::group_type_builder("schema")
                .with_fields(&mut fields)
                .build()
                .map_storage_err()?,
        ))
    }
}
