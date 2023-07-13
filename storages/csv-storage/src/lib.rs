pub mod error;
mod store;
mod store_mut;

use {
    error::{CsvStorageError, ResultExt},
    gluesql_core::{
        ast::{ColumnUniqueOption, DataType},
        data::{Key, Schema, Value},
        error::{Error, Result},
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, Metadata,
            RowIter, Transaction,
        },
    },
    std::{
        collections::HashMap,
        fs::{self, File},
        io::Read,
        path::PathBuf,
    },
};

pub struct CsvStorage {
    pub path: PathBuf,
}

impl CsvStorage {
    pub fn new(path: &str) -> Result<Self> {
        fs::create_dir_all(path).map_storage_err()?;
        let path = PathBuf::from(path);

        Ok(Self { path })
    }

    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let schema_path = self.schema_path(table_name);
        if !schema_path.exists() {
            return Ok(None);
        }

        let mut file = File::open(&schema_path).map_storage_err()?;
        let mut ddl = String::new();
        file.read_to_string(&mut ddl).map_storage_err()?;

        let schema = Schema::from_ddl(&ddl)?;
        if schema.table_name != table_name {
            return Err(Error::StorageMsg(
                CsvStorageError::TableNameDoesNotMatchWithFile.to_string(),
            ));
        }

        Ok(Some(schema))
    }

    fn path_by(&self, table_name: &str, extension: &str) -> PathBuf {
        let path = self.path.as_path();
        let mut path = path.join(table_name);
        path.set_extension(extension);

        path
    }

    fn schema_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "sql")
    }

    fn data_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "csv")
    }

    fn scan_data(&self, table_name: &str) -> Result<(Option<Vec<String>>, RowIter)> {
        let schema = self.fetch_schema(table_name)?;
        let schema2 = schema.clone();

        let data_path = self.data_path(table_name);

        let columns = match schema {
            Some(Schema {
                column_defs: Some(column_defs),
                ..
            }) => column_defs
                .into_iter()
                .map(|column_def| column_def.name)
                .collect::<Vec<_>>(),
            _ if data_path.exists() => {
                let mut rdr = csv::Reader::from_path(data_path).map_storage_err()?;

                rdr.headers()
                    .map_storage_err()?
                    .into_iter()
                    .map(|header| header.to_string())
                    .collect()
            }
            _ => {
                return Ok((None, Box::new(std::iter::empty())));
            }
        };

        let data_path = self.data_path(table_name);
        let rdr = csv::Reader::from_path(data_path).map_storage_err()?;

        let cc = columns.clone();
        let rows = rdr.into_records().enumerate().map(move |(index, record)| {
            let column_defs = schema2
                .as_ref()
                .and_then(|schema| schema.column_defs.as_ref());

            let record = record.map_storage_err()?;
            let get_index_key = || index.try_into().map(Key::I64).map_storage_err();

            let column_defs = match column_defs {
                Some(column_defs) => column_defs,
                None => {
                    let key = get_index_key()?;

                    let row = record
                        .into_iter()
                        .zip(cc.clone().into_iter())
                        .map(|(value, column)| {
                            let value = match value {
                                "NULL" => Value::Null,
                                _ => Value::Str(value.to_owned()),
                            };

                            Ok((column, value))
                        })
                        /*
                        .map(String::from)
                        .map(Value::Str)
                        */
                        .collect::<Result<HashMap<String, Value>>>()?;
                    let row = DataRow::Map(row);

                    return Ok((key, row));
                }
            };

            let mut key: Option<Key> = None;
            let values = record
                .into_iter()
                .zip(column_defs)
                .map(|(value, column_def)| {
                    let value = match value {
                        "NULL" => Value::Null,
                        _ => Value::Str(value.to_owned()),
                    };

                    let value = match &column_def.data_type {
                        DataType::Text => value,
                        data_type => value.cast(data_type)?,
                    };

                    if column_def.unique == Some(ColumnUniqueOption { is_primary: true }) {
                        key = Key::try_from(&value).map(Some)?;
                    }

                    Ok(value)
                })
                .collect::<Result<Vec<Value>>>()?;

            let key = key.map(Ok).unwrap_or_else(get_index_key)?;
            let row = DataRow::Vec(values);

            Ok((key, row))
        });

        Ok((Some(columns), Box::new(rows)))
    }
}

impl AlterTable for CsvStorage {}
impl CustomFunction for CsvStorage {}
impl CustomFunctionMut for CsvStorage {}
impl Index for CsvStorage {}
impl IndexMut for CsvStorage {}
impl Transaction for CsvStorage {}
impl Metadata for CsvStorage {}
