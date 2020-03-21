use bincode;
use nom_sql::CreateTableStatement;
use sled::{self, Db, IVec};

use crate::data::Row;
use crate::error::*;
use crate::storage::Store;

pub struct SledStorage {
    tree: Db,
}

impl SledStorage {
    pub fn new(filename: String) -> Result<Self> {
        let tree = sled::open(filename)?;

        Ok(Self { tree })
    }
}

impl Store<IVec> for SledStorage {
    fn gen_id(&self, table_name: &str) -> Result<IVec> {
        let id = format!("data/{}/{}", table_name, self.tree.generate_id()?);

        Ok(IVec::from(id.as_bytes()))
    }

    fn set_schema(&self, statement: &CreateTableStatement) -> Result<()> {
        let k = format!("schema/{}", statement.table.name);
        let k = k.as_bytes();
        let v: Vec<u8> = bincode::serialize(&statement)?;

        self.tree.insert(k, v)?;

        Ok(())
    }

    fn get_schema(&self, table_name: &str) -> Result<CreateTableStatement> {
        let k = format!("schema/{}", table_name);
        let k = k.as_bytes();
        let v: &[u8] = &self.tree.get(&k)?.ok_or(Error::NotFound)?;
        let statement = bincode::deserialize(v)?;

        Ok(statement)
    }

    fn set_data(&self, key: &IVec, row: Row) -> Result<Row> {
        let v: Vec<u8> = bincode::serialize(&row)?;

        self.tree.insert(key, v)?;

        Ok(row)
    }

    fn get_data(&self, table_name: &str) -> Result<Box<dyn Iterator<Item = (IVec, Row)>>> {
        let prefix = format!("data/{}/", table_name);

        let result_set = self
            .tree
            .scan_prefix(prefix.as_bytes())
            .map(|result| result.expect("should be unwrapped"))
            .map(move |(key, value)| (key, bincode::deserialize(&value).expect("Stop iterate")));

        Ok(Box::new(result_set))
    }

    fn del_data(&self, key: &IVec) -> Result<()> {
        self.tree.remove(key)?;

        Ok(())
    }
}
