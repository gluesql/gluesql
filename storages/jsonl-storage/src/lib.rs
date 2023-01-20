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
    path: PathBuf,
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

impl JsonlStorage {
    pub fn new(path: &str) -> Result<Self> {
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

    fn table_path(&self, table_name: &str) -> Result<PathBuf> {
        let schema = self
            .tables
            .get(table_name)
            .ok_or_else(|| Error::StorageMsg("table does not exist".to_owned()))?;
        let path = format!("{}/{}.json", self.path.display(), schema.table_name);

        Ok(PathBuf::from(path))
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
        let mut file = File::create(path).unwrap();
        write!(file, "{ddl}").unwrap();

        Ok(())
    }
}

#[async_trait(?Send)]
impl Store for JsonlStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        Ok(self.tables.get(table_name).map(ToOwned::to_owned))
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        Ok(self.tables.iter().map(|table| table.1.to_owned()).collect())
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        let row = self
            .scan_data(table_name)
            .await?
            .find_map(|result| Some(result.map(|(key, row)| (&key == target).then_some(row))));

        match row {
            Some(row) => row,
            None => todo!(),
        }
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let path = JsonlStorage::table_path(self, table_name)?;

        match read_lines(path) {
            Ok(lines) => {
                let row_iter = lines.enumerate().map(|(key, line)| -> Result<_> {
                    let hash_map = HashMap::parse_json_object(&line.map_storage_err()?);
                    let data_row = DataRow::Map(hash_map?);
                    let key = Key::Uuid(key.try_into().map_storage_err()?);

                    Ok((key, data_row))
                });

                Ok(Box::new(row_iter))
            }
            Err(_) => todo!("error reading json file"),
        }
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

        match &schema.column_defs {
            Some(columns) => self.write_schema(schema),
            None => todo!(),
        }
        .unwrap();

        match File::create(path).map_storage_err() {
            Ok(_) => {
                let mut storage = self;
                JsonlStorage::insert_schema(&mut storage, schema);

                Ok((storage, ()))
            }
            Err(e) => return Err((self, e)),
        }
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let table_path = JsonlStorage::table_path(&self, table_name);
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
            Err(e) => Err((self, e)),
        }
    }

    async fn append_data(self, table_name: &str, rows: Vec<DataRow>) -> MutResult<Self, ()> {
        let result = self
            .tables
            .get(table_name)
            .ok_or_else(|| Error::StorageMsg("could not find table".to_owned()))
            .and_then(|schema| {
                let table_path = JsonlStorage::table_path(&self, table_name)?;

                let mut file = OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(&table_path)
                    .map_err(|_| Error::StorageMsg("could not open file".to_owned()))?;

                for row in rows {
                    match row {
                        DataRow::Map(hash_map) => {
                            let mut json = hash_map
                                .iter()
                                .map(|(k, v)| format!("\"{k}\": {}", String::from(v)))
                                .collect::<Vec<_>>();
                            json.sort(); // todo! remove sort?
                            let json = json.join(", ");
                            write!(file, "{{{json}}}\n").map_storage_err()?;
                        }
                        DataRow::Vec(values) => {
                            match &schema.column_defs {
                                Some(column_defs) => {
                                    // todo! validate columns
                                    let mut json = column_defs
                                        .iter()
                                        .map(|column_def| column_def.name.clone())
                                        .zip(values.iter())
                                        .map(|(k, v)| format!("\"{k}\": {}", String::from(v))) // todo! enclose string with ''?
                                        .collect::<Vec<_>>();
                                    json.sort();
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
        let prev_rows = self.scan_data(table_name).await.unwrap();

        // todo! impl without sort + vector.zip
        let prev_rows = prev_rows
            .collect::<Result<HashMap<Key, DataRow>>>()
            .unwrap();

        let rows = prev_rows.concat(rows.into_iter());
        let mut rows = rows.into_iter().collect::<Vec<_>>();

        rows.sort_by(|(key_a, _), (key_b, _)| match (key_a, key_b) {
            (Key::Uuid(a), Key::Uuid(b)) => a.cmp(b),
            _ => todo!(),
        });

        let rows = rows.into_iter().map(|(_, data_row)| data_row).collect();

        let table_path = JsonlStorage::table_path(&self, table_name).unwrap();
        File::create(&table_path).map_storage_err().unwrap();
        self.append_data(table_name, rows).await
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        let prev_rows = self.scan_data(table_name).await.unwrap();
        let rows = prev_rows
            .filter_map(|result| match result {
                Ok((key, data_row)) => match keys.iter().any(|target_key| target_key == &key) {
                    true => None,
                    false => Some(data_row),
                },
                Err(_) => None, // todo! how not to ignore error?
            })
            .collect::<Vec<_>>();

        let table_path = JsonlStorage::table_path(&self, table_name).unwrap();
        File::create(&table_path).map_storage_err().unwrap();
        self.append_data(table_name, rows).await
    }
}

#[test]
fn jsonl_storage_test() {
    let path = ".";
    let jsonl_storage = JsonlStorage::new(path).unwrap();
    let table_name = "Items".to_string();
    // let path = PathBuf::from(format!("{path}/{table_name}.json"));
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
