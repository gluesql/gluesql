pub mod error;
mod store;
mod store_mut;

use {
    error::{CsvStorageError, ResultExt},
    gluesql_core::{
        ast::{ColumnDef, ColumnUniqueOption, DataType},
        data::{Key, Schema, Value},
        error::Result,
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

    fn fetch_schema(&self, table_name: &str) -> Result<Option<(Schema, bool)>> {
        let schema_path = self.schema_path(table_name);
        if !schema_path.exists() {
            let data_path = self.data_path(table_name);
            let types_path = self.types_path(table_name);

            let column_defs = match (types_path.exists(), data_path.exists()) {
                (false, false) => return Ok(None),
                (false, true) => Some(
                    csv::Reader::from_path(data_path)
                        .map_storage_err()?
                        .headers()
                        .map_storage_err()?
                        .into_iter()
                        .map(|header| ColumnDef {
                            name: header.to_string(),
                            data_type: DataType::Text,
                            unique: None,
                            default: None,
                            nullable: true,
                        })
                        .collect::<Vec<_>>(),
                ),
                (true, _) => None,
            };

            let schema = Schema {
                table_name: table_name.to_owned(),
                column_defs,
                indexes: Vec::new(),
                engine: None,
            };

            return Ok(Some((schema, true)));
        }

        let mut file = File::open(&schema_path).map_storage_err()?;
        let mut ddl = String::new();
        file.read_to_string(&mut ddl).map_storage_err()?;

        let schema = Schema::from_ddl(&ddl)?;
        if schema.table_name != table_name {
            return Err(CsvStorageError::TableNameDoesNotMatchWithFile.into());
        }

        Ok(Some((schema, false)))
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

    fn tmp_data_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "tmp.csv")
    }

    fn types_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "types.csv")
    }

    fn tmp_types_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "types.tmp.csv")
    }

    fn scan_data(&self, table_name: &str) -> Result<(Option<Vec<String>>, RowIter)> {
        let data_path = self.data_path(table_name);
        let (schema, generated) = match (self.fetch_schema(table_name)?, data_path.exists()) {
            (None, _) | (_, false) => return Ok((None, Box::new(std::iter::empty()))),
            (Some(v), true) => v,
        };

        let mut data_rdr = csv::Reader::from_path(data_path).map_storage_err()?;
        let mut fetch_data_header_columns = || -> Result<Vec<String>> {
            Ok(data_rdr
                .headers()
                .map_storage_err()?
                .into_iter()
                .map(|header| header.to_string())
                .collect::<Vec<_>>())
        };

        if let Schema {
            column_defs: Some(column_defs),
            ..
        } = schema
        {
            let columns = column_defs
                .iter()
                .map(|column_def| column_def.name.to_owned())
                .collect::<Vec<_>>();

            let rows = data_rdr
                .into_records()
                .enumerate()
                .map(move |(index, record)| {
                    let mut key: Option<Key> = None;

                    let values = record
                        .map_storage_err()?
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

                    let key = key.unwrap_or(Key::U64(index as u64));
                    let row = DataRow::Vec(values);

                    Ok((key, row))
                });

            Ok((Some(columns), Box::new(rows)))
        } else if self.types_path(table_name).exists() {
            let types_path = self.types_path(table_name);
            let types_rdr = csv::Reader::from_path(types_path)
                .map_storage_err()?
                .into_records();

            let columns = fetch_data_header_columns()?;
            let rows = data_rdr.into_records().zip(types_rdr).enumerate().map(
                move |(index, (record, types))| {
                    let key = Key::U64(index as u64);
                    let record = record.map_storage_err()?;
                    let types = types.map_storage_err()?;

                    record
                        .into_iter()
                        .zip(columns.iter())
                        .zip(&types)
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

                            Some(value.map(|value| (column.clone(), value)))
                        })
                        .collect::<Result<HashMap<String, Value>>>()
                        .map(DataRow::Map)
                        .map(|row| (key, row))
                },
            );

            Ok((None, Box::new(rows)))
        } else {
            let columns = fetch_data_header_columns()?;
            let rows = {
                let columns = columns.clone();

                data_rdr
                    .into_records()
                    .enumerate()
                    .map(move |(index, record)| {
                        let key = Key::U64(index as u64);
                        let row = record
                            .map_storage_err()?
                            .into_iter()
                            .zip(columns.iter())
                            .map(|(value, column)| (column.clone(), Value::Str(value.to_owned())))
                            .collect::<HashMap<String, Value>>();

                        Ok((key, DataRow::Map(row)))
                    })
            };

            Ok((generated.then_some(columns), Box::new(rows)))
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
