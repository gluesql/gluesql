mod alter_table;
pub mod error;
mod index;
mod store;
mod store_mut;
mod transaction;

use {
    error::{JsonlStorageError, OptionExt, ResultExt},
    gluesql_core::{
        ast::ColumnUniqueOption,
        chrono::NaiveDateTime,
        data::{value::HashMapJsonExt, Schema},
        prelude::{Key, Value},
        result::{Error, Result},
        store::{DataRow, RowIter},
    },
    iter_enum::Iterator,
    serde_json::Value as JsonValue,
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
        match (
            self.jsonl_path(table_name).exists(),
            self.json_path(table_name).exists(),
        ) {
            (true, true) => {
                return Err(Error::StorageMsg(
                    JsonlStorageError::BothJsonlAndJsonExist(table_name.to_owned()).to_string(),
                ))
            }
            (false, false) => return Ok(None),
            _ => {}
        }

        let schema_path = self.schema_path(table_name);
        let column_defs = match schema_path.exists() {
            true => {
                let mut file = File::open(&schema_path).map_storage_err()?;
                let mut ddl = String::new();
                file.read_to_string(&mut ddl).map_storage_err()?;

                let schema = Schema::from_ddl(&ddl)?;
                if schema.table_name != table_name {
                    return Err(Error::StorageMsg(
                        JsonlStorageError::TableNameDoesNotMatchWithFile.to_string(),
                    ));
                }

                schema.column_defs
            }
            false => None,
        };

        Ok(Some(Schema {
            table_name: table_name.to_owned(),
            column_defs,
            indexes: vec![],
            created: NaiveDateTime::default(),
            engine: None,
        }))
    }

    fn jsonl_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "jsonl")
    }

    fn json_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "json")
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

        let json_path = self.json_path(table_name);

        #[derive(Iterator)]
        enum Jsons<I1, I2> {
            A(I1),
            B(I2),
        }

        let jsons = match fs::read_to_string(json_path) {
            Ok(json_file_str) => {
                let value = serde_json::from_str(&json_file_str).map_err(|_| {
                    Error::StorageMsg(
                        JsonlStorageError::InvalidJsonString(json_file_str.to_owned()).to_string(),
                    )
                })?;

                let jsons = match value {
                    JsonValue::Array(values) => values
                        .into_iter()
                        .map(|value| match value {
                            JsonValue::Object(json_map) => HashMap::try_from_json_map(json_map),
                            _ => Err(Error::StorageMsg(
                                JsonlStorageError::JsonObjectTypeRequired.to_string(),
                            )),
                        })
                        .collect::<Result<Vec<_>>>(),
                    JsonValue::Object(json_map) => Ok(vec![HashMap::try_from_json_map(json_map)?]),
                    _ => Err(Error::StorageMsg(
                        JsonlStorageError::JsonArrayTypeRequired.to_string(),
                    )),
                }?;

                Jsons::A(jsons.into_iter().map(Ok))
            }
            Err(_) => {
                let jsonl_path = self.jsonl_path(table_name);
                let lines = read_lines(jsonl_path).map_storage_err()?;

                let jsons = lines.map(|line| HashMap::parse_json_object(&line.map_storage_err()?));

                Jsons::B(jsons)
            }
        };

        let rows = jsons.enumerate().map(move |(index, json)| -> Result<_> {
            let json = json?;
            let row = json_to_row(index, json, &schema)?;

            Ok(row)
        });

        Ok(Box::new(rows))
    }
}

fn json_to_row(
    index: usize,
    json_row: HashMap<String, Value>,
    schema: &Schema,
) -> Result<(Key, DataRow)> {
    let get_index_key = || index.try_into().map(Key::I64).map_storage_err();

    let column_defs = match &schema.column_defs {
        Some(column_defs) => column_defs,
        None => {
            let key = get_index_key()?;
            let row = DataRow::Map(json_row);

            return Ok((key, row));
        }
    };

    let mut key: Option<Key> = None;
    let mut values = Vec::with_capacity(column_defs.len());
    for column_def in column_defs {
        let value = json_row.get(&column_def.name).map_storage_err(
            JsonlStorageError::ColumnDoesNotExist(column_def.name.clone()),
        )?;

        if column_def.unique == Some(ColumnUniqueOption { is_primary: true }) {
            key = Some(value.clone().try_into().map_storage_err()?);
        }

        let value = match value.get_type() {
            Some(data_type) if data_type != column_def.data_type => {
                value.cast(&column_def.data_type)?
            }
            Some(_) | None => value.clone(),
        };

        values.push(value);
    }

    let key = match key {
        Some(key) => key,
        None => get_index_key()?,
    };
    let row = DataRow::Vec(values);

    Ok((key, row))
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
