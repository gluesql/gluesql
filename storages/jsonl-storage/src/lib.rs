use std::{
    collections::HashMap,
    fs::{self, read_to_string, File},
    io::{self, BufRead},
    path::{Path, PathBuf},
};

use gluesql_core::{chrono::NaiveDateTime, prelude::Value, store::StoreMut};

use {
    async_trait::async_trait,
    gluesql_core::{
        data::{HashMapJsonExt, Schema},
        prelude::Key,
        result::{Error, Result},
        store::{DataRow, RowIter, Store},
    },
};

pub struct JsonlStorage {
    tables: HashMap<String, JsonlTable>,
}

impl JsonlStorage {
    pub fn new(directory: &str) -> Result<Self> {
        let paths = fs::read_dir(directory).unwrap();
        let tables = paths
            .into_iter()
            .map(|result| {
                let path = result.unwrap().path();
                let table_name = path.file_name().unwrap().to_str().unwrap().to_owned();
                let jsonl_table = JsonlTable::new(table_name.clone(), path);

                (table_name, jsonl_table)
            })
            .collect::<HashMap<String, JsonlTable>>();

        Ok(Self { tables })
    }
}

struct JsonlTable {
    schema: Schema,
    path: PathBuf,
}

impl JsonlTable {
    fn new(table_name: String, path: PathBuf) -> Self {
        let schema = Schema {
            table_name,
            column_defs: None,
            indexes: vec![],
            created: NaiveDateTime::default(),
        };

        Self { schema, path }
    }
}

#[async_trait(?Send)]
impl Store for JsonlStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        Ok(self
            .tables
            .get(table_name)
            .map(|table| table.schema.to_owned()))
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        Ok(self
            .tables
            .iter()
            .map(|table| table.1.schema.to_owned())
            .collect())
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
        let path = self.tables.get(table_name).map(|table| &table.path);

        match read_lines(path.unwrap()) {
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

#[async_trait(?Send)]
impl StoreMut for JsonlStorage {
    async fn insert_schema(self, schema: &Schema) -> gluesql_core::result::MutResult<Self, ()> {
        todo!()
    }

    async fn delete_schema(self, table_name: &str) -> gluesql_core::result::MutResult<Self, ()> {
        todo!()
    }

    async fn append_data(
        self,
        table_name: &str,
        rows: Vec<DataRow>,
    ) -> gluesql_core::result::MutResult<Self, ()> {
        todo!()
    }

    async fn insert_data(
        self,
        table_name: &str,
        rows: Vec<(Key, DataRow)>,
    ) -> gluesql_core::result::MutResult<Self, ()> {
        todo!()
    }

    async fn delete_data(
        self,
        table_name: &str,
        keys: Vec<Key>,
    ) -> gluesql_core::result::MutResult<Self, ()> {
        todo!()
    }
}
