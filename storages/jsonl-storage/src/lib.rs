mod alter_table;
pub mod error;
mod index;
mod store;
mod store_mut;
mod transaction;

use {
    error::{JsonlStorageError, OptionExt, ResultExt},
    gluesql_core::{
        ast::ColumnDef,
        chrono::NaiveDateTime,
        data::{value::HashMapJsonExt, Schema},
        prelude::Key,
        result::Result,
        store::{DataRow, RowIter},
    },
    std::{
        collections::HashMap,
        fs::{self, File},
        io::{self, BufRead, Read},
        path::{Path, PathBuf},
    },
};

pub struct JsonlStorage {
    pub path: PathBuf,
}

impl JsonlStorage {
    pub fn new(path: &str) -> Result<Self> {
        fs::create_dir_all(path).map_storage_err()?;
        let path = PathBuf::from(path);

        Ok(Self { path })
    }

    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        if !self.data_path(table_name).exists() {
            return Ok(None);
        };

        let schema_path = self.schema_path(table_name);
        let column_defs = match schema_path.exists() {
            true => {
                let mut file = File::open(&schema_path).map_storage_err()?;
                let mut ddl = String::new();
                file.read_to_string(&mut ddl).map_storage_err()?;

                Schema::from_ddl(&ddl).map(|schema| schema.column_defs)
            }
            false => Ok(None),
        }?;

        Ok(Some(Schema {
            table_name: table_name.to_owned(),
            column_defs,
            indexes: vec![],
            created: NaiveDateTime::default(),
            engine: None,
        }))
    }

    fn data_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "jsonl")
    }

    fn schema_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "sql")
    }

    fn path_by(&self, table_name: &str, extension: &str) -> PathBuf {
        let path = self.path.as_path();
        let mut path = path.join(table_name);
        path.set_extension(extension);

        path
    }

    fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let schema = self
            .fetch_schema(table_name)?
            .map_storage_err(JsonlStorageError::TableDoesNotExist)?;
        let data_path = self.data_path(table_name);
        let lines = read_lines(data_path).map_storage_err()?;

        let row_iter = lines.enumerate().map(move |(key, line)| -> Result<_> {
            let hash_map = HashMap::parse_json_object(&line.map_storage_err()?)?;

            let mut key_temp: Option<Key> = None;
            let data_row = match &schema.column_defs {
                Some(column_defs) => {
                    let mut values = vec![];
                    for column_def in column_defs {
                        let value = hash_map.get(&column_def.name).map_storage_err(
                            JsonlStorageError::ColumnDoesNotExist(column_def.name.clone()),
                        )?;

                        if column_def
                            .unique
                            .map(|column_unique_option| column_unique_option.is_primary)
                            .unwrap_or(false)
                        {
                            key_temp = Some(value.clone().try_into().map_storage_err()?);
                        }

                        let value = match value.get_type() {
                            Some(data_type) if data_type != column_def.data_type => {
                                value.cast(&column_def.data_type)?
                            }
                            Some(_) | None => value.clone(),
                        };

                        values.push(value);
                    }

                    // let values = column_defs
                    //     .iter()
                    //     .map(|column_def| -> Result<_> {
                    //         let value = hash_map.get(&column_def.name).map_storage_err(
                    //             JsonlStorageError::ColumnDoesNotExist(column_def.name.clone()),
                    //         )?;

                    //         match value.get_type() {
                    //             Some(data_type) if data_type != column_def.data_type => {
                    //                 value.cast(&column_def.data_type)
                    //             }
                    //             Some(_) | None => Ok(value.clone()),
                    //         }
                    //     })
                    //     .collect::<Result<Vec<_>>>()?;

                    DataRow::Vec(values)
                }
                None => DataRow::Map(hash_map.clone()),
            };
            let key = match key_temp {
                Some(key) => key,
                None => (key + 1).try_into().map_storage_err().map(Key::I64)?,
            };
            // let key = match &primary_key {
            //     Some(primary_key) => hash_map.get(primary_key).unwrap().try_into()?,
            //     None => Key::I64((key + 1).try_into().map_storage_err()?),
            // };

            Ok((key, data_row))
        });

        Ok(Box::new(row_iter))
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn get_primary_key(column_defs: &Option<Vec<ColumnDef>>) -> Option<String> {
    column_defs
        .as_ref()
        .and_then(|column_defs| {
            column_defs.iter().find(|column_def| {
                column_def
                    .unique
                    .map(|column_unique_option| column_unique_option.is_primary)
                    .unwrap_or(false)
            })
        })
        .map(|column_def| column_def.name.to_owned())
}
