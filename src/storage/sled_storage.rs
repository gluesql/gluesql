use bincode;
use nom_sql::CreateTableStatement;
use sled::{self, Db, IVec};
use thiserror::Error as ThisError;

use crate::data::Row;
use crate::result::{Error, Result};
use crate::storage::{Store, StoreError};

#[derive(ThisError, Debug)]
enum StorageError {
    #[error(transparent)]
    Store(#[from] StoreError),

    #[error(transparent)]
    Sled(#[from] sled::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
}

impl Into<Error> for StorageError {
    fn into(self) -> Error {
        use StorageError::*;

        match self {
            Sled(e) => Error::Storage(Box::new(e)),
            Bincode(e) => Error::Storage(e),
            Store(e) => e.into(),
        }
    }
}

type SledResult<T> = std::result::Result<T, StorageError>;

pub struct SledStorage {
    tree: Db,
}

impl SledStorage {
    pub fn new(filename: String) -> Result<Self> {
        let tree = sled::open(filename).map_err(|e| {
            let e: StorageError = e.into();
            let e: Error = e.into();

            e
        })?;

        Ok(Self { tree })
    }

    fn impl_gen_id(&self, table_name: &str) -> SledResult<IVec> {
        let id = format!("data/{}/{}", table_name, self.tree.generate_id()?);

        Ok(IVec::from(id.as_bytes()))
    }

    fn impl_set_schema(&self, statement: &CreateTableStatement) -> SledResult<()> {
        let k = format!("schema/{}", statement.table.name);
        let k = k.as_bytes();
        let v: Vec<u8> = bincode::serialize(&statement)?;

        self.tree.insert(k, v)?;

        Ok(())
    }

    fn impl_get_schema(&self, table_name: &str) -> SledResult<CreateTableStatement> {
        let k = format!("schema/{}", table_name);
        let k = k.as_bytes();
        let v: &[u8] = &self.tree.get(&k)?.ok_or(StoreError::SchemaNotFound)?;
        let statement = bincode::deserialize(v)?;

        Ok(statement)
    }

    fn impl_set_data(&self, key: &IVec, row: Row) -> SledResult<Row> {
        let v: Vec<u8> = bincode::serialize(&row)?;

        self.tree.insert(key, v)?;

        Ok(row)
    }

    fn impl_get_data(&self, table_name: &str) -> SledResult<Box<dyn Iterator<Item = (IVec, Row)>>> {
        let prefix = format!("data/{}/", table_name);

        let result_set = self
            .tree
            .scan_prefix(prefix.as_bytes())
            .map(|result| result.expect("should be unwrapped"))
            .map(move |(key, value)| (key, bincode::deserialize(&value).expect("Stop iterate")));

        Ok(Box::new(result_set))
    }

    fn impl_del_data(&self, key: &IVec) -> SledResult<()> {
        self.tree.remove(key)?;

        Ok(())
    }
}

impl Store<IVec> for SledStorage {
    fn gen_id(&self, table_name: &str) -> Result<IVec> {
        self.impl_gen_id(table_name).map_err(|e| e.into())
    }

    fn set_schema(&self, statement: &CreateTableStatement) -> Result<()> {
        self.impl_set_schema(statement).map_err(|e| e.into())
    }

    fn get_schema(&self, table_name: &str) -> Result<CreateTableStatement> {
        self.impl_get_schema(table_name).map_err(|e| e.into())
    }

    fn set_data(&self, key: &IVec, row: Row) -> Result<Row> {
        self.impl_set_data(key, row).map_err(|e| e.into())
    }

    fn get_data(&self, table_name: &str) -> Result<Box<dyn Iterator<Item = (IVec, Row)>>> {
        self.impl_get_data(table_name).map_err(|e| e.into())
    }

    fn del_data(&self, key: &IVec) -> Result<()> {
        self.impl_del_data(key).map_err(|e| e.into())
    }
}
