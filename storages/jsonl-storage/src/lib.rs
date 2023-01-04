mod alter_table;
mod index;
mod transaction;

use std::{
    collections::HashMap,
    fs::{self, read_to_string, File, OpenOptions},
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
};

use futures::executor::block_on;
use gluesql_core::{chrono::NaiveDateTime, prelude::Value, result::MutResult, store::StoreMut};
use std::io::prelude::*;

use {
    async_trait::async_trait,
    gluesql_core::{
        data::{HashMapJsonExt, Schema},
        prelude::Key,
        result::{Error, Result},
        store::{DataRow, RowIter, Store},
    },
};

#[derive(Debug, Default)]
pub struct JsonlStorage {
    tables: HashMap<String, Schema>,
    path: PathBuf,
}

impl JsonlStorage {
    pub fn new(path: &str) -> Result<Self> {
        let paths = fs::read_dir(path).unwrap();
        let tables = paths
            .into_iter()
            .map(|result| {
                let path = result.unwrap().path();
                let table_name = path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned()
                    .replace(".json", "");

                let jsonl_table = JsonlStorage::new_table(table_name.clone());

                (table_name, jsonl_table)
            })
            .collect::<HashMap<String, Schema>>();

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

    fn table_path(&self, table_name: &str) -> PathBuf {
        let schema = self.tables.get(table_name).unwrap(); // table does not exist
        let path = format!("{}/{}.json", self.path.display(), schema.table_name);

        PathBuf::from(path)
    }

    fn insert_schema(&mut self, schema: &Schema) {
        let json_table = JsonlStorage::new_table(schema.table_name.clone());
        self.tables.insert(schema.table_name.clone(), json_table);
    }

    pub fn delete_schema(&mut self, table_name: &str) {
        self.tables.remove(table_name);
    }
}

// #[derive(Debug)]
// struct JsonlTable {
//     schema: Schema,
//     path: PathBuf,
// }

// impl JsonlTable {
//     fn new(table_name: String, path: PathBuf) -> Self {
//             table_name,
//         let schema = Schema {
//             column_defs: None,
//             indexes: vec![],
//             created: NaiveDateTime::default(),
//         };

//         Self { schema, path }
//     }
// }

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
            .await
            .unwrap()
            .find_map(|result| Some(result.map(|(key, row)| (&key == target).then_some(row))));

        match row {
            Some(row) => row,
            None => todo!(),
        }
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let path = JsonlStorage::table_path(self, table_name);

        match read_lines(path) {
            Ok(lines) => {
                let row_iter = lines.enumerate().map(|(key, line)| -> Result<_> {
                    let hash_map = HashMap::parse_json_object(&line.unwrap());
                    let data_row = DataRow::Map(hash_map?);
                    let key = Key::Uuid(key.try_into().unwrap());

                    Ok((key, data_row))
                });

                Ok(Box::new(row_iter))
            }
            Err(_) => todo!(),
        }
        // let data = read_to_string(path.unwrap());

        // let data = match path {
        //     Some(path) => read_to_string(path),
        //     None => panic!(),
        // };

        // let data = data.unwrap();
        // let lines = data.lines();

        // let row_iter = lines.enumerate().map(|(key, line)| -> Result<_> {
        //     let hash_map = HashMap::parse_json_object(line);
        //     let data_row = DataRow::Map(hash_map?);
        //     let key = Key::Uuid(key.try_into().unwrap());

        //     Ok((key, data_row))
        // });

        // Ok(Box::new(row_iter))
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

impl JsonlStorage {}

#[async_trait(?Send)]
impl StoreMut for JsonlStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let path = format!("{}/{}.json", self.path.display(), schema.table_name);
        let path = PathBuf::from(path);

        File::create(path).unwrap();

        let mut storage = self;
        JsonlStorage::insert_schema(&mut storage, schema);

        Ok((storage, ()))
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let mut storage = self;
        JsonlStorage::delete_schema(&mut storage, table_name);

        Ok((storage, ()))
    }

    async fn append_data(self, table_name: &str, rows: Vec<DataRow>) -> MutResult<Self, ()> {
        self.tables.get(table_name).and_then(|jsonl_table| {
            // let file = File::open(jsonl_table.path).unwrap();
            // let buffered = BufReader::new(file);

            // for line in buffered.lines() {
            //     println!("{}", line.unwrap());
            // }
            let table_path = JsonlStorage::table_path(&self, table_name);

            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(&table_path)
                .unwrap();

            rows.iter().for_each(|row| {
                let row_to_json = match row {
                    DataRow::Vec(values) => todo!(),
                    //     values.iter().fold("", |acc, cur| {
                    //     // let a: String = acc.into();
                    //     // let b: String = cur.into();
                    // }),
                    DataRow::Map(hash_map) => {
                        let json = serde_json::to_string(hash_map).unwrap();
                        write!(file, "{}", json);
                    }
                };

                // if let Err(e) = writeln!(file, row) {
                //     eprintln!("Couldn't write to file: {}", e);
                // }
            });

            Some(())
        });

        Ok((self, ()))
    }

    async fn insert_data(self, table_name: &str, rows: Vec<(Key, DataRow)>) -> MutResult<Self, ()> {
        todo!()
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        todo!()
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
