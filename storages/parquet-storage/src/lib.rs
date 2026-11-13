use {
    error::{OptionExt, ParquetStorageError, ResultExt},
    gluesql_core::{
        ast::{ColumnDef, ColumnUniqueOption},
        data::Schema,
        error::Result,
        prelude::{DataType, Key, Value},
        store::{Metadata, Planner, ScanColumns, Store},
    },
    parquet::{
        file::{reader::FileReader, serialized_reader::SerializedFileReader},
        record::{Row, reader::RowIter as ParquetRowIter},
        schema::types::Type as ParquetType,
    },
    std::{
        collections::BTreeMap,
        fs::{self, File},
        path::{Path, PathBuf},
    },
    value::ParquetField,
};

mod alter_table;
mod column_def;
pub mod error;
mod function;
mod index;
mod store;
mod store_mut;
mod transaction;
mod value;

type RowIter = Box<dyn Iterator<Item = Result<(Key, Vec<Value>)>>>;
const ENGINE_METADATA_KEY: &str = "gluesql.engine";

#[derive(Debug, Clone)]
pub struct ParquetStorage {
    pub path: PathBuf,
}

impl ParquetStorage {
    /// Create a parquet storage rooted at the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created.
    pub fn new<T: AsRef<Path>>(path: T) -> Result<Self> {
        let path = path.as_ref();
        fs::create_dir_all(path).map_storage_err()?;

        Ok(Self { path: path.into() })
    }

    fn data_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "parquet")
    }

    fn path_by(&self, table_name: &str, extension: &str) -> PathBuf {
        let path = self.path.as_path();
        let mut path = path.join(table_name);
        path.set_extension(extension);

        path
    }

    fn scan_data(&self, table_name: &str, columns: ScanColumns<'_>) -> Result<(RowIter, Schema)> {
        let fetched_schema = <Self as Store>::fetch_schema(self, table_name)?.map_storage_err(
            ParquetStorageError::TableDoesNotExist(table_name.to_owned()),
        )?;
        let file = File::open(self.data_path(table_name)).map_storage_err()?;

        let parquet_reader = SerializedFileReader::new(file).map_storage_err()?;

        if fetched_schema.column_defs.is_none() {
            let rows = Self::scan_schemaless(parquet_reader);
            return Ok((rows, fetched_schema));
        }

        let scan_schema = fetched_schema.clone();
        let primary_key_index = scan_schema.column_defs.as_ref().and_then(|column_defs| {
            column_defs.iter().position(|column_def| {
                column_def.unique == Some(ColumnUniqueOption { is_primary: true })
            })
        });

        let (projected_schema, read_column_indexes, unrequested_primary_key_index) = match columns {
            ScanColumns::All => (None, None, None),
            ScanColumns::Selected(requested_columns) => {
                let mut read_column_indexes = requested_columns
                    .iter()
                    .map(|(_, index)| *index)
                    .collect::<Vec<_>>();
                let mut unrequested_primary_key_index = None;

                if let Some(index) = primary_key_index
                    && let Err(position) = read_column_indexes.binary_search(&index)
                {
                    read_column_indexes.insert(position, index);
                    unrequested_primary_key_index = Some(index);
                }

                let file_schema = parquet_reader.metadata().file_metadata().schema();
                let mut fields = read_column_indexes
                    .iter()
                    .map(|index| file_schema.get_fields()[*index].clone())
                    .collect();
                let projected_schema = ParquetType::group_type_builder(file_schema.name())
                    .with_fields(&mut fields)
                    .build()
                    .map_storage_err()?;

                (
                    Some(projected_schema),
                    Some(read_column_indexes),
                    unrequested_primary_key_index,
                )
            }
        };
        let row_iter = ParquetRowIter::from_file_into(Box::new(parquet_reader))
            .project(projected_schema)
            .map_storage_err()?;
        let mut key_counter = 0_u64;

        let rows: RowIter = Box::new(row_iter.map(move |record| {
            let record = record.map_storage_err()?;
            let mut row = Vec::new();
            let mut key = None;

            for (index, (_, field)) in record.get_column_iter().enumerate() {
                let original_index = read_column_indexes
                    .as_ref()
                    .map_or(index, |indexes| indexes[index]);
                let value = ParquetField(field).to_value(&scan_schema, original_index)?;

                if primary_key_index == Some(original_index) {
                    key = Key::try_from(&value).ok();
                }

                if unrequested_primary_key_index != Some(original_index) {
                    row.push(value);
                }
            }

            let key = key.unwrap_or_else(|| {
                let generated = Key::U64(key_counter);
                key_counter += 1;
                generated
            });

            Ok((key, row))
        }));

        Ok((rows, fetched_schema))
    }

    fn scan_schemaless(parquet_reader: SerializedFileReader<File>) -> RowIter {
        let row_iter = ParquetRowIter::from_file_into(Box::new(parquet_reader));
        let tmp_schema = Self::generate_temp_schema();
        let mut key_counter = 0_u64;

        Box::new(row_iter.flat_map(move |record| {
            let rows = record.map_storage_err().and_then(|record: Row| {
                let mut data_map = BTreeMap::new();
                let mut rows = Vec::new();

                for (_, field) in record.get_column_iter() {
                    let value = ParquetField(field).to_value(&tmp_schema, 0)?;
                    let generated_key = Key::U64(key_counter);
                    key_counter += 1;
                    if let Value::Map(inner_map) = value {
                        data_map = inner_map;
                    }

                    rows.push((generated_key, vec![Value::Map(data_map.clone())]));
                }

                Ok(rows)
            });

            match rows {
                Ok(rows) => rows.into_iter().map(Ok).collect::<Vec<_>>().into_iter(),
                Err(error) => vec![Err(error)].into_iter(),
            }
        }))
    }

    fn generate_temp_schema() -> Schema {
        Schema {
            table_name: "temporary".to_owned(),
            column_defs: Some(vec![ColumnDef {
                name: "schemaless".to_owned(),
                data_type: DataType::Map,
                nullable: true,
                default: None,
                unique: None,
                comment: None,
            }]),
            indexes: vec![],
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        }
    }
}

impl Metadata for ParquetStorage {}
impl Planner for ParquetStorage {}

#[cfg(test)]
mod tests {
    use {
        super::*,
        gluesql_core::prelude::Error,
        parquet::{
            basic::{Repetition, Type},
            column::writer::ColumnWriter,
            data_type::ByteArray,
            file::{properties::WriterProperties, writer::SerializedFileWriter},
            format::KeyValue,
            schema::types::Type as SchemaType,
        },
        std::{
            fs::{File, remove_dir_all},
            sync::Arc,
        },
        uuid::Uuid,
    };

    #[test]
    fn scan_data_returns_schemaless_value_conversion_errors() {
        let path = std::env::temp_dir().join(format!("parquet-storage-{}", Uuid::new_v4()));
        let storage = ParquetStorage::new(&path).expect("create parquet storage");
        let field = SchemaType::primitive_type_builder("schemaless", Type::BYTE_ARRAY)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("build parquet field");
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(&mut vec![Arc::new(field)])
            .build()
            .map(Arc::new)
            .expect("build parquet schema");
        let properties = Arc::new(
            WriterProperties::builder()
                .set_key_value_metadata(Some(vec![KeyValue {
                    key: "schemaless".to_owned(),
                    value: Some("true".to_owned()),
                }]))
                .build(),
        );
        let file = File::create(storage.data_path("Foo")).expect("create parquet file");
        let mut file_writer =
            SerializedFileWriter::new(file, schema, properties).expect("create parquet writer");

        {
            let mut row_group = file_writer.next_row_group().expect("create row group");
            let mut column = row_group
                .next_column()
                .expect("read column")
                .expect("expected column");

            match column.untyped() {
                ColumnWriter::ByteArrayColumnWriter(writer) => writer
                    .write_batch(&[ByteArray::from(vec![0_u8])], Some(&[1]), None)
                    .expect("write invalid serialized map"),
                _ => panic!("expected byte array column"),
            };

            column.close().expect("close column");
            row_group.close().expect("close row group");
        }

        file_writer.close().expect("close parquet writer");

        let (mut rows, _) = storage
            .scan_data("Foo", ScanColumns::All)
            .expect("scan data");
        let error = rows
            .next()
            .expect("conversion error row")
            .expect_err("invalid schemaless map should fail to deserialize");

        assert!(matches!(error, Error::StorageMsg(_)));
        remove_dir_all(path).expect("remove temporary storage");
    }
}
