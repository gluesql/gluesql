#![deny(clippy::str_to_string)]

mod core;
mod error;

use {
    async_trait::async_trait,
    core::StorageCore,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, Metadata,
            Planner, RowIter, Store, StoreMut, Transaction,
        },
    },
    redb::Database,
    std::path::Path,
};

pub struct RedbStorage(StorageCore);

impl RedbStorage {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        StorageCore::new(filename).map(Self).map_err(Into::into)
    }

    pub fn from_database(db: Database) -> Self {
        Self(StorageCore::from_database(db))
    }
}

#[async_trait]
impl Store for RedbStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        self.0.fetch_all_schemas().map_err(Into::into)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.0.fetch_schema(table_name).map_err(Into::into)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        self.0.fetch_data(table_name, key).map_err(Into::into)
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        self.0.scan_data(table_name).map_err(Into::into)
    }
}

#[async_trait]
impl StoreMut for RedbStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        self.0.insert_schema(schema).map_err(Into::into)
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.0.delete_schema(table_name).map_err(Into::into)
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        self.0.append_data(table_name, rows).map_err(Into::into)
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        self.0.insert_data(table_name, rows).map_err(Into::into)
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        self.0.delete_data(table_name, keys).map_err(Into::into)
    }
}

#[async_trait]
impl Transaction for RedbStorage {
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        self.0.begin(autocommit).map_err(Into::into)
    }

    async fn rollback(&mut self) -> Result<()> {
        self.0.rollback().map_err(Into::into)
    }

    async fn commit(&mut self) -> Result<()> {
        self.0.commit().map_err(Into::into)
    }
}

impl AlterTable for RedbStorage {}
impl Index for RedbStorage {}
impl IndexMut for RedbStorage {}
impl Metadata for RedbStorage {}
impl CustomFunction for RedbStorage {}
impl CustomFunctionMut for RedbStorage {}
impl Planner for RedbStorage {}
