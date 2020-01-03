use bincode;
use sled::Db;
use nom_sql::CreateTableStatement;

use crate::storage::Store;

pub struct SledStorage {
    tree: Db,
}

impl SledStorage {
    pub fn new(filename: String) -> SledStorage {
        let tree = Db::open(filename).unwrap();

        SledStorage { tree }
    }
}

impl Store for SledStorage {
    fn set_schema(&self, statement: CreateTableStatement) -> Result<(), ()> {
        let tree = &self.tree;

        println!("\nSledStorage setSchema! {:#?}", statement);
        println!("table name is {}", statement.table.name);

        let k = format!("schema/{}", &statement.table.name);
        println!("key name is {}", k);
        let k = k.as_bytes();
        let v: Vec<u8> = bincode::serialize(&statement).unwrap();

        match tree.insert(k, v) {
            Ok(_) => { println!("insert table succeeded"); },
            Err(_) => { println!("insert failed"); },
        }

        Ok(())
    }

    fn get_schema(&self, table_name: String) -> Result<CreateTableStatement, &str> {
        let tree = &self.tree;

        println!("\nSledStorage getSchema! {}", table_name);

        let k = format!("schema/{}", table_name);
        println!("key name is {}", k);
        let k = k.as_bytes();
        let v: &[u8] = &tree.get(&k).unwrap().unwrap();
        let statement = bincode::deserialize(v).unwrap();

        Ok(statement)
    }
}
