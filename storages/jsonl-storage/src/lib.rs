mod alter_table;
mod index;
mod transaction;

use {
    async_trait::async_trait,
    gluesql_core::{
        data::{HashMapJsonExt, Schema},
        prelude::Key,
        result::{Error, Result},
        store::{DataRow, RowIter, Store},
        {chrono::NaiveDateTime, store::StoreMut},
    },
    iter_enum::Iterator,
    serde_json::Value as JsonValue,
    std::{
        collections::HashMap,
        fmt,
        fs::{self, remove_file, File, OpenOptions},
        io::{self, prelude::*, BufRead},
        path::{Path, PathBuf},
    },
    utils::HashMapExt,
};

#[derive(Debug)]
pub struct JsonlStorage {
    tables: HashMap<String, Schema>,
    pub path: PathBuf,
}

impl Default for JsonlStorage {
    fn default() -> Self {
        JsonlStorage {
            tables: HashMap::new(),
            path: PathBuf::from("data"),
        }
    }
}

trait ResultExt<T, E: ToString> {
    fn map_storage_err(self) -> Result<T, Error>;
}

impl<T, E: ToString> ResultExt<T, E> for std::result::Result<T, E> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

trait OptionExt<T> {
    fn map_storage_err(self, payload: String) -> Result<T, Error>;
}

impl<T> OptionExt<T> for std::option::Option<T> {
    fn map_storage_err(self, payload: String) -> Result<T, Error> {
        self.ok_or_else(|| payload.to_string())
            .map_err(Error::StorageMsg)
    }
}

enum JsonlStorageError {
    FileNotFound,
    CannotConvertToString,
    TableDoesNotExist,
    ColumnDoesNotExist,
}

impl fmt::Display for JsonlStorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let payload = match self {
            JsonlStorageError::FileNotFound => "file not found",
            JsonlStorageError::CannotConvertToString => "cannot convert to string",
            JsonlStorageError::TableDoesNotExist => "table does not exist",
            JsonlStorageError::ColumnDoesNotExist => "column does not exist",
        };

        write!(f, "{}", payload)
    }
}

impl JsonlStorage {
    pub fn new(path: &str) -> Result<Self> {
        fs::create_dir_all(path).map_storage_err()?;
        let paths = fs::read_dir(path).map_storage_err()?;
        let tables = paths
            .filter(|result| {
                result
                    .as_ref()
                    .map(|dir_entry| {
                        dir_entry
                            .path()
                            .extension()
                            .map(|os_str| os_str.to_str() == Some("json"))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false)
            })
            .map(|result| -> Result<_> {
                let path = result.map_storage_err()?.path();
                let table_name = path
                    .file_name()
                    .map_storage_err(JsonlStorageError::FileNotFound.to_string())?
                    .to_str()
                    .map_storage_err(JsonlStorageError::CannotConvertToString.to_string())?
                    .to_owned()
                    .replace(".json", "");

                let jsonl_table = JsonlStorage::new_table(table_name.clone());

                Ok((table_name, jsonl_table))
            })
            .collect::<Result<HashMap<String, Schema>>>()?;

        let path = PathBuf::from(path);

        Ok(Self { tables, path })
    }

    fn new_table(table_name: String) -> Schema {
        Schema {
            table_name,
            column_defs: None,
            indexes: vec![],
            created: NaiveDateTime::default(),
        }
    }

    fn data_path(&self, table_name: &str) -> Result<PathBuf> {
        let path = self.path_by(table_name, "json")?;

        Ok(PathBuf::from(path))
    }

    fn schema_path(&self, table_name: &str) -> Option<PathBuf> {
        let path = self.path_by(table_name, "sql").ok();

        path.map(PathBuf::from)
    }

    fn path_by(&self, table_name: &str, extension: &str) -> Result<String, Error> {
        let schema = self
            .tables
            .get(table_name)
            .map_storage_err(JsonlStorageError::TableDoesNotExist.to_string())?;
        let path = format!("{}/{}.{extension}", self.path.display(), schema.table_name);

        Ok(path)
    }

    fn insert_schema(&mut self, schema: &Schema) {
        self.tables
            .insert(schema.table_name.clone(), schema.to_owned());
    }

    pub fn delete_schema(&mut self, table_name: &str) {
        self.tables.remove(table_name);
    }

    fn write_schema(&self, schema: &Schema) -> Result<()> {
        let path = format!("{}/{}.sql", self.path.display(), schema.table_name);
        let ddl = schema.clone().to_ddl();
        let mut file = File::create(path).map_storage_err()?;
        write!(file, "{ddl}").map_storage_err()?;

        Ok(())
    }

    fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let schema = self
            .tables
            .get(table_name)
            .map_storage_err(JsonlStorageError::TableDoesNotExist.to_string())?
            .to_owned();
        let data_path = self.data_path(table_name)?;
        let lines = read_lines(data_path).map_storage_err()?;
        let row_iter = lines.enumerate().map(move |(key, line)| -> Result<_> {
            let hash_map = HashMap::parse_json_object(&line.map_storage_err()?)?;
            let data_row = match schema.clone().column_defs {
                Some(column_defs) => {
                    let values = column_defs
                        .iter()
                        .map(|column_def| -> Result<_> {
                            let value = hash_map
                                .get(&column_def.name)
                                .map_storage_err(JsonlStorageError::ColumnDoesNotExist.to_string())?
                                .clone();
                            let data_type = value.get_type();
                            match data_type {
                                Some(data_type) => match data_type == column_def.data_type {
                                    true => Ok(value),
                                    false => value.cast(&column_def.data_type),
                                },
                                None => Ok(value),
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;

                    DataRow::Vec(values)
                }
                None => DataRow::Map(hash_map),
            };
            let key = Key::I64((key + 1).try_into().map_storage_err()?);

            Ok((key, data_row))
        });

        Ok(Box::new(row_iter))
    }
}

#[async_trait(?Send)]
impl Store for JsonlStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        Ok(self.tables.get(table_name).map(ToOwned::to_owned))
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut vec = self
            .tables
            .iter()
            .map(|table| table.1.to_owned())
            .collect::<Vec<_>>();
        // vec.sort();
        vec.sort_by(|key_a, key_b| key_a.table_name.cmp(&key_b.table_name));

        Ok(vec)
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        let row = self.scan_data(table_name)?.find_map(|result| {
            result
                .map(|(key, row)| (&key == target).then_some(row))
                .unwrap_or(None)
        });

        Ok(row)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        self.scan_data(table_name)
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

#[async_trait(?Send)]
impl StoreMut for JsonlStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let path = format!("{}/{}.json", self.path.display(), schema.table_name);
        let path = PathBuf::from(path);

        if schema.column_defs.is_some() {
            self.write_schema(schema)?
        }

        File::create(path).map_storage_err()?;
        JsonlStorage::insert_schema(self, schema);

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        if let Ok(table_path) = JsonlStorage::data_path(self, table_name) {
            let schema_path = JsonlStorage::schema_path(self, table_name);

            remove_file(table_path).map_storage_err()?;
            if let Some(schema_path) = schema_path {
                remove_file(schema_path).map_storage_err()?;
            }

            JsonlStorage::delete_schema(self, table_name);
        }

        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        #[derive(Iterator)]
        enum JsonIter<I1, I2> {
            Map(I1),
            Vec(I2),
        }

        self.tables
            .get(table_name)
            .map_storage_err(JsonlStorageError::TableDoesNotExist.to_string())
            .and_then(|schema| {
                let table_path = JsonlStorage::data_path(self, table_name)?;

                let mut file = OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(table_path)
                    .map_storage_err()?;

                for row in rows {
                    let json_string = match row {
                        DataRow::Map(hash_map) => JsonIter::Map(hash_map.into_iter()),
                        DataRow::Vec(values) => {
                            match &schema.column_defs {
                                Some(column_defs) => {
                                    // todo! validate columns?
                                    JsonIter::Vec(
                                        column_defs
                                            .iter()
                                            .map(|column_def| column_def.name.clone())
                                            .zip(values.into_iter()),
                                    )
                                }
                                None => break, // todo! looks like unreachable
                            }
                        }
                    }
                    .map(|(key, value)| {
                        let value = JsonValue::try_from(value)?.to_string();

                        Ok(format!("\"{key}\": {value}"))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");

                    writeln!(file, "{{{json_string}}}").map_storage_err()?;
                }

                Ok(())
            })
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        let prev_rows = self.scan_data(table_name)?;

        // todo! impl without sort + vector.zip
        let prev_rows = prev_rows.collect::<Result<HashMap<Key, DataRow>>>()?;

        let rows = prev_rows.concat(rows.into_iter());
        let mut rows = rows.into_iter().collect::<Vec<_>>();

        rows.sort_by(|(key_a, _), (key_b, _)| {
            key_a
                .partial_cmp(key_b)
                .unwrap_or(std::cmp::Ordering::Equal) // todo! okay to be equal?
        });

        let rows = rows.into_iter().map(|(_, data_row)| data_row).collect();

        let table_path = JsonlStorage::data_path(self, table_name)?;
        File::create(&table_path).map_storage_err()?;

        self.append_data(table_name, rows).await
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let prev_rows = self.scan_data(table_name)?;
        let rows = prev_rows
            .filter_map(|result| {
                result
                    .map(|(key, data_row)| {
                        let preservable = !keys.iter().any(|target_key| target_key == &key);

                        preservable.then_some(data_row)
                    })
                    .unwrap_or(None)
                // todo! how not to ignore error?
                // can remove result from RowIter?
            })
            .collect::<Vec<_>>();

        let table_path = JsonlStorage::data_path(self, table_name)?;
        File::create(&table_path).map_storage_err()?;

        self.append_data(table_name, rows).await
    }
}

#[test]
fn jsonl_storage_test() {
    use futures::executor::block_on;

    let path = ".";
    let mut jsonl_storage = JsonlStorage::new(path).unwrap();
    let table_name = "Items".to_string();
    let schema = Schema {
        table_name: table_name.clone(),
        column_defs: None,
        indexes: Vec::new(),
        created: NaiveDateTime::default(),
    };
    block_on(async {
        jsonl_storage.insert_schema(&schema);
        let actual = jsonl_storage
            .fetch_schema(&table_name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, schema);
    });
}
