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

macro_rules! try_into {
    ($expr: expr) => {
        $expr.map_err(|e| {
            let e: StorageError = e.into();
            let e: Error = e.into();

            e
        })?
    };
}

pub struct SledStorage {
    tree: Db,
}

impl SledStorage {
    pub fn new(filename: String) -> Result<Self> {
        let tree = try_into!(sled::open(filename));

        Ok(Self { tree })
    }
}

impl Store<IVec> for SledStorage {
    fn gen_id(&self, table_name: &str) -> Result<IVec> {
        let id = try_into!(self.tree.generate_id());
        let id = format!("data/{}/{}", table_name, id);

        Ok(IVec::from(id.as_bytes()))
    }

    fn set_schema(&self, statement: &CreateTableStatement) -> Result<()> {
        let key = format!("schema/{}", statement.table.name);
        let key = key.as_bytes();
        let value = try_into!(bincode::serialize(&statement));

        try_into!(self.tree.insert(key, value));

        Ok(())
    }

    fn get_schema(&self, table_name: &str) -> Result<CreateTableStatement> {
        let key = format!("schema/{}", table_name);
        let key = key.as_bytes();
        let value = try_into!(self.tree.get(&key));
        let value = value.ok_or(StoreError::SchemaNotFound)?;
        let statement = try_into!(bincode::deserialize(&value));

        Ok(statement)
    }

    fn set_data(&self, key: &IVec, row: Row) -> Result<Row> {
        let value = try_into!(bincode::serialize(&row));

        try_into!(self.tree.insert(key, value));

        Ok(row)
    }

    fn get_data(&self, table_name: &str) -> Result<Box<dyn Iterator<Item = Result<(IVec, Row)>>>> {
        let prefix = format!("data/{}/", table_name);

        let result_set = self.tree.scan_prefix(prefix.as_bytes()).map(move |item| {
            let (key, value) = try_into!(item);
            let value = try_into!(bincode::deserialize(&value));

            Ok((key, value))
        });

        Ok(Box::new(result_set))
    }

    fn del_data(&self, key: &IVec) -> Result<()> {
        try_into!(self.tree.remove(key));

        Ok(())
    }
}
