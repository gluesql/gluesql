use gluesql_core::{
    prelude::{DataType, Value},
    result::TrySelf,
};

mod alter_table;
mod index;
mod schema;
mod transaction;

use {
    async_trait::async_trait,
    futures::executor::block_on,
    gluesql_core::{
        data::{HashMapJsonExt, Schema},
        prelude::Key,
        result::{Error, Result},
        store::{DataRow, RowIter, Store},
        {chrono::NaiveDateTime, result::MutResult, store::StoreMut},
    },
    serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue},
    std::{
        collections::HashMap,
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
    fn map_storage_err(self, payload: &str) -> Result<T, Error>;
}

impl<T> OptionExt<T> for std::option::Option<T> {
    fn map_storage_err(self, payload: &str) -> Result<T, Error> {
        self.ok_or_else(|| payload.to_string())
            .map_err(Error::StorageMsg)
    }
}

impl JsonlStorage {
    pub fn new(path: &str) -> Result<Self> {
        fs::create_dir_all(path).map_storage_err()?;
        let paths = fs::read_dir(path).map_storage_err()?;
        let tables = paths
            .into_iter()
            .map(|result| -> Result<_> {
                let path = result.map_err(|e| Error::StorageMsg(e.to_string()))?.path();
                let table_name = path
                    .file_name()
                    .ok_or_else(|| Error::StorageMsg("file not found".to_owned()))?
                    .to_str()
                    .ok_or_else(|| Error::StorageMsg("cannot convert to string".to_owned()))?
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

    fn schema_path(&self, table_name: &str) -> Result<PathBuf> {
        let path = self.path_by(table_name, "sql")?;

        Ok(PathBuf::from(path))
    }

    fn path_by(&self, table_name: &str, extension: &str) -> Result<String, Error> {
        let schema = self
            .tables
            .get(table_name)
            .ok_or_else(|| Error::StorageMsg("table does not exist".to_owned()))?;
        let path = format!("{}/{}.{extension}", self.path.display(), schema.table_name);
        Ok(path)
    }

    fn insert_schema(&mut self, schema: &Schema) {
        // let json_table = JsonlStorage::new_table(schema.table_name.clone());
        self.tables
            .insert(schema.table_name.clone(), schema.to_owned());
    }

    pub fn delete_schema(&mut self, table_name: &str) {
        self.tables.remove(table_name);
    }

    // pub fn load_table(&self, table_name: String, column_defs: Vec<gluesql_core::ast::ColumnDef>) {
    //     let schema = Schema {
    //         table_name,
    //         column_defs: Some(column_defs),
    //         indexes: Vec::new(),
    //         created: NaiveDateTime::default(), // todo!: parse comment
    //     };
    // }

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
            .map_storage_err("table does not exist")?
            .to_owned();
        let data_path = self.data_path(table_name)?;

        match read_lines(data_path) {
            Ok(lines) => {
                let row_iter = lines.enumerate().map(move |(key, line)| -> Result<_> {
                    // way1. DataRow::Vec try_into each column type
                    // way2. parse UUID prefix like X'..'
                    let hash_map = HashMap::parse_json_object(&line.map_storage_err()?)?;
                    let data_row = match schema.clone().column_defs {
                        Some(column_defs) => {
                            let values = column_defs
                                .iter()
                                .map(|column_def| -> Result<_> {
                                    // data_type, key, value => 1. data_type::try_from(value)
                                    let value = hash_map
                                        .get(&column_def.name)
                                        .map_storage_err("column does not exist")?
                                        .clone();
                                    let data_type = value.get_type();
                                    match data_type {
                                        Some(data_type) => {
                                            match data_type == column_def.data_type {
                                                true => Ok(value),
                                                false => value.cast(&column_def.data_type),
                                            }
                                        }
                                        None => Ok(value),
                                    }
                                })
                                .collect::<Result<Vec<_>>>()?;

                            DataRow::Vec(values)
                        }
                        None => {
                            // let hash_map = HashMap::parse_json_object(&line.map_storage_err()?)?;
                            DataRow::Map(hash_map)
                        }
                    };
                    // todo! okay not to use UUID?
                    // todo! line starts from 1?
                    let key = Key::I64((key + 1).try_into().map_storage_err()?);

                    Ok((key, data_row))
                });

                Ok(Box::new(row_iter))
            }
            Err(_) => todo!("error reading json file"),
        }
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
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let path = format!("{}/{}.json", self.path.display(), schema.table_name);
        let path = PathBuf::from(path);

        let storage = match &schema.column_defs {
            Some(_) => self.write_schema(schema).try_self(self)?.0,
            _ => self,
        };

        let (mut storage, _) = File::create(path).map_storage_err().try_self(storage)?;
        JsonlStorage::insert_schema(&mut storage, schema);

        Ok((storage, ()))
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        // todo! should delete including .sql file
        let table_path = JsonlStorage::data_path(&self, table_name);
        match table_path {
            Ok(table_path) => {
                match remove_file(table_path).map_storage_err() {
                    Ok(_) => {}
                    Err(e) => return Err((self, e)),
                }

                let mut storage = self;
                JsonlStorage::delete_schema(&mut storage, table_name);

                return Ok((storage, ()));
            }
            Err(_) => Ok((self, ())), // todo! fair enough to squash error for drop table if exist?
        }
    }

    async fn append_data(self, table_name: &str, rows: Vec<DataRow>) -> MutResult<Self, ()> {
        let result = self
            .tables
            .get(table_name)
            .ok_or_else(|| Error::StorageMsg("could not find table".to_owned()))
            .and_then(|schema| {
                let table_path = JsonlStorage::data_path(&self, table_name)?;

                let mut file = OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(&table_path)
                    .map_err(|e| Error::StorageMsg(e.to_string()))?;

                for row in rows {
                    match row {
                        DataRow::Map(hash_map) => {
                            let json = hash_map
                                .into_iter()
                                // todo! why even schemaless get to here? on_where.rs L55
                                .map(|(key, value)| {
                                    let value = JsonValue::try_from(value)?.to_string();

                                    Ok(format!("\"{key}\": {value}"))
                                })
                                .collect::<Result<Vec<_>>>()?;
                            // json.sort(); // todo! remove sort?
                            let json = json.join(", ");
                            write!(file, "{{{json}}}\n").map_storage_err()?;
                        }
                        DataRow::Vec(values) => {
                            match &schema.column_defs {
                                Some(column_defs) => {
                                    // todo! validate columns
                                    let json = column_defs
                                        .iter()
                                        .map(|column_def| column_def.name.clone())
                                        .zip(values.into_iter())
                                        .map(|(key, value)| {
                                            let value = JsonValue::try_from(value)?.to_string();

                                            Ok(format!("\"{key}\": {value}"))
                                        })
                                        .collect::<Result<Vec<_>>>()?;
                                    // json.sort();
                                    let json = json.join(", ");
                                    write!(file, "{{{json}}}\n").map_storage_err()?;
                                }
                                None => unreachable!(),
                            };
                        }
                    }
                }

                Ok(())
            });

        match result {
            Ok(_) => Ok((self, ())),
            Err(e) => Err((self, e)),
        }
    }

    async fn insert_data(self, table_name: &str, rows: Vec<(Key, DataRow)>) -> MutResult<Self, ()> {
        let (self, prev_rows) = self.scan_data(table_name).try_self(self)?;

        // todo! impl without sort + vector.zip
        let (self, prev_rows) = prev_rows
            .collect::<Result<HashMap<Key, DataRow>>>()
            .try_self(self)?;

        let rows = prev_rows.concat(rows.into_iter());
        let mut rows = rows.into_iter().collect::<Vec<_>>();

        rows.sort_by(|(key_a, _), (key_b, _)| {
            key_a
                .partial_cmp(key_b)
                .unwrap_or(std::cmp::Ordering::Equal) // todo! okay to be equal?
        });

        let rows = rows.into_iter().map(|(_, data_row)| data_row).collect();

        let (self, table_path) = JsonlStorage::data_path(&self, table_name).try_self(self)?;
        let (self, _) = File::create(&table_path).map_storage_err().try_self(self)?;

        self.append_data(table_name, rows).await
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        let (self, prev_rows) = self.scan_data(table_name).try_self(self)?;
        let rows = prev_rows
            .filter_map(|result| match result {
                Ok((key, data_row)) => match keys.iter().any(|target_key| target_key == &key) {
                    true => None,
                    false => Some(data_row),
                },
                Err(_) => None, // todo! how not to ignore error?
            })
            .collect::<Vec<_>>();

        let (self, table_path) = JsonlStorage::data_path(&self, table_name).try_self(self)?;
        let (self, _) = File::create(&table_path).map_storage_err().try_self(self)?;

        self.append_data(table_name, rows).await
    }
}

#[test]
fn jsonl_storage_test() {
    let path = ".";
    let jsonl_storage = JsonlStorage::new(path).unwrap();
    let table_name = "Items".to_string();
    let schema = Schema {
        table_name: table_name.clone(),
        column_defs: None,
        indexes: Vec::new(),
        created: NaiveDateTime::default(),
    };
    block_on(async {
        let (jsonl_storage, _) = jsonl_storage.insert_schema(&schema).await.unwrap();
        let actual = jsonl_storage
            .fetch_schema(&table_name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, schema);
    });
}
