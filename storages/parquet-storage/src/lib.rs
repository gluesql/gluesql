use {
    error::{OptionExt, ParquetStorageError, ResultExt},
    gluesql_core::{
        ast::{ColumnDef, ColumnUniqueOption},
        data::Schema,
        error::Result,
        prelude::{DataType, Key, Value},
        store::{Metadata, Planner, Store},
    },
    parquet::{
        file::{reader::FileReader, serialized_reader::SerializedFileReader},
        record::Row,
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

    fn scan_data(&self, table_name: &str) -> Result<(RowIter, Schema)> {
        let fetched_schema = <Self as Store>::fetch_schema(self, table_name)?.map_storage_err(
            ParquetStorageError::TableDoesNotExist(table_name.to_owned()),
        )?;
        let file = File::open(self.data_path(table_name)).map_storage_err()?;

        let parquet_reader = SerializedFileReader::new(file).map_storage_err()?;
        let row_iter = parquet_reader.get_row_iter(None).map_storage_err()?;

        let mut rows = Vec::new();
        let mut key_counter: u64 = 0;

        if let Some(column_defs) = &fetched_schema.column_defs {
            for record in row_iter {
                let record: Row = record.map_storage_err()?;
                let mut row = Vec::new();
                let mut key = None;

                for (idx, (_, field)) in record.get_column_iter().enumerate() {
                    let value = ParquetField(field.clone()).to_value(&fetched_schema, idx)?;
                    row.push(value.clone());

                    if column_defs[idx].unique == Some(ColumnUniqueOption { is_primary: true }) {
                        key = Key::try_from(&value).ok();
                    }
                }

                let generated_key = key.unwrap_or_else(|| {
                    let generated = Key::U64(key_counter);
                    key_counter += 1;
                    generated
                });
                rows.push(Ok((generated_key, row)));
            }
        } else {
            let tmp_schema = Self::generate_temp_schema();
            for record in row_iter {
                let record: Row = record.map_storage_err()?;
                let mut data_map = BTreeMap::new();

                for (_, field) in record.get_column_iter() {
                    let value = ParquetField(field.clone()).to_value(&tmp_schema, 0)?;
                    let generated_key = Key::U64(key_counter);
                    key_counter += 1;
                    if let Value::Map(inner_map) = value {
                        data_map = inner_map;
                    }

                    rows.push(Ok((generated_key, vec![Value::Map(data_map.clone())])));
                }
            }
        }

        Ok((Box::new(rows.into_iter()), fetched_schema))
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
