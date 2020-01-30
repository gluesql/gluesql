use bincode;
use nom_sql::CreateTableStatement;
use sled::{self, Db};

use crate::storage::Store;
use crate::translator::Row;

pub struct SledStorage {
    tree: Db,
}

impl SledStorage {
    pub fn new(filename: String) -> SledStorage {
        let tree = sled::open(filename).unwrap();

        SledStorage { tree }
    }
}

impl Store<u64> for SledStorage {
    fn gen_id(&self) -> Result<u64, ()> {
        let id = self.tree.generate_id().unwrap();

        Ok(id)
    }

    fn set_schema(&self, statement: &CreateTableStatement) -> Result<(), ()> {
        let k = format!("schema/{}", statement.table.name);
        let k = k.as_bytes();
        let v: Vec<u8> = bincode::serialize(&statement).unwrap();

        self.tree.insert(k, v).unwrap();

        Ok(())
    }

    fn get_schema(&self, table_name: &str) -> Result<CreateTableStatement, &str> {
        let k = format!("schema/{}", table_name);
        let k = k.as_bytes();
        let v: &[u8] = &self.tree.get(&k).unwrap().unwrap();
        let statement = bincode::deserialize(v).unwrap();

        Ok(statement)
    }

    fn set_data(&self, table_name: &str, row: Row<u64>) -> Result<Row<u64>, ()> {
        let k = format!("data/{}/{}", table_name, row.key);
        let k = k.as_bytes();
        let v: Vec<u8> = bincode::serialize(&row).unwrap();

        self.tree.insert(k, v).unwrap();

        Ok(row)
    }

    fn get_data(&self, table_name: &str) -> Result<Box<dyn Iterator<Item = Row<u64>>>, ()> {
        let k = format!("data/{}/", table_name);
        let k = k.as_bytes();

        let result_set = self
            .tree
            .scan_prefix(k)
            .map(|result| result.expect("should be unwrapped"))
            .map(|(_, value)| bincode::deserialize(&value).expect("Stop iterate"));

        Ok(Box::new(result_set))
    }

    fn del_data(&self, table_name: &str, key: &u64) -> Result<(), ()> {
        let k = format!("data/{}/{}", table_name, key);
        let k = k.as_bytes();

        self.tree.remove(k).unwrap();

        Ok(())
    }
}
