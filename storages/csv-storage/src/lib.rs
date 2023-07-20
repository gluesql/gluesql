pub mod error;
mod store;
mod store_mut;

use {
    error::{CsvStorageError, ResultExt},
    gluesql_core::{
        ast::{ColumnUniqueOption, DataType},
        data::{Key, Schema, Value},
        error::{Error, Result},
        parse_sql::parse_data_type,
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, Metadata,
            RowIter, Transaction,
        },
        translate::translate_data_type,
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

    fn types_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "types.csv")
    }

    fn scan_data(&self, table_name: &str) -> Result<(Option<Vec<String>>, RowIter)> {
        let schema = self.fetch_schema(table_name)?;
        let data_path = self.data_path(table_name);
        if !data_path.exists() {
            return Ok((None, Box::new(std::iter::empty())));
        }

        let mut data_rdr = csv::Reader::from_path(data_path).map_storage_err()?;

        /*
          1. schema file exists
            yes -> then
                1-1. schema
                1-2. schemaless
            no -> fetch column based on data file
                all column types are string
          2. data file exists
            yes -> fetch
            no -> error
          3. types file exists
            yes -> use it
            no -> .. ok

        let's flatten this

          1. data file does not exist
            -> error
            -> now data file exists

          2. schema file exists

          2-1. column_def exists
            don't use types file
            use column_def

          2-2. column_def does not exist

          2-2-1. types file exists
            use types file

          2-2-2. types file does not exist
            fetch columns from data file
            all the types are string

          3. schema file does not exist

            3-1. types file exists
                use types file

            3-2. types file does not exist
                fetch columns from data file
                all the types are string

        ## finalization

        1. column_defs field exists
        2. types file exists
        3. neither column_defs nor types file exists
        */

        if let Some(Schema {
            column_defs: Some(column_defs),
            ..
        }) = schema
        {
            let columns = column_defs
                .iter()
                .map(|column_def| column_def.name.to_owned())
                .collect::<Vec<_>>();

            let rows = data_rdr
                .into_records()
                .enumerate()
                .map(move |(index, record)| {
                    let record = record.map_storage_err()?;
                    let get_index_key = || index.try_into().map(Key::I64).map_storage_err();

                    let mut key: Option<Key> = None;
                    let values = record
                        .into_iter()
                        .zip(column_defs.iter())
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
        } else if self.types_path(table_name).exists() {
            let types_path = self.types_path(table_name);
            let types_rdr = csv::Reader::from_path(types_path)
                .map_storage_err()?
                .into_records();

            let columns: Vec<_> = data_rdr
                .headers()
                .map_storage_err()?
                .into_iter()
                .map(|header| header.to_string())
                .collect();

            let rows = data_rdr.into_records().zip(types_rdr).enumerate().map(
                move |(index, (record, types))| {
                    let key = Key::U64(index as u64);
                    let record = record.map_storage_err()?;
                    let types = types.map_storage_err()?;

                    record
                        .into_iter()
                        .zip(columns.clone().into_iter())
                        .zip(types.into_iter())
                        .filter_map(|((value, column), data_type)| {
                            if data_type.is_empty() {
                                return None;
                            }

                            let value = if data_type == "NULL" {
                                Ok(Value::Null)
                            } else {
                                parse_data_type(data_type).and_then(|data_type| {
                                    let data_type = translate_data_type(&data_type)?;
                                    let value = Value::Str(value.to_owned());

                                    match data_type {
                                        DataType::Text => Ok(value),
                                        data_type => value.cast(&data_type),
                                    }
                                })
                            };

                            Some(value.map(|value| (column, value)))
                        })
                        .collect::<Result<HashMap<String, Value>>>()
                        .map(DataRow::Map)
                        .map(|row| (key, row))
                },
            );

            Ok((None, Box::new(rows)))
        } else {
            let columns = data_rdr
                .headers()
                .map_storage_err()?
                .into_iter()
                .map(|header| header.to_string())
                .collect::<Vec<_>>();

            let rows = data_rdr
                .into_records()
                .enumerate()
                .map(move |(index, record)| {
                    let key = Key::U64(index as u64);
                    let record = record.map_storage_err()?;

                    let row = record
                        .into_iter()
                        .map(|value| Value::Str(value.to_owned()))
                        .collect::<Vec<Value>>();

                    Ok((key, DataRow::Vec(row)))
                });

            Ok((Some(columns), Box::new(rows)))
        }
    }
}

impl AlterTable for CsvStorage {}
impl CustomFunction for CsvStorage {}
impl CustomFunctionMut for CsvStorage {}
impl Index for CsvStorage {}
impl IndexMut for CsvStorage {}
impl Transaction for CsvStorage {}
impl Metadata for CsvStorage {}
