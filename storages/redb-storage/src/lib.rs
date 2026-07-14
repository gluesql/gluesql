#![deny(clippy::str_to_string)]

mod core;
mod error;
mod index;
mod index_mut;
mod index_sync;
mod migration;
mod planner;

pub use migration::{MigrationReport, REDB_STORAGE_FORMAT_VERSION, migrate_to_latest};

use {
    core::StorageCore,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result,
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, Metadata, RowIter, Store, StoreMut,
            Transaction,
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

    pub fn from_database(db: Database) -> Result<Self> {
        StorageCore::from_database(db).map(Self).map_err(Into::into)
    }
}

impl Store for RedbStorage {
    fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        self.0.fetch_all_schemas().map_err(Into::into)
    }

    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.0.fetch_schema(table_name).map_err(Into::into)
    }

    fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
        self.0.fetch_data(table_name, key).map_err(Into::into)
    }

    fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        let rows = self.0.scan_data(table_name)?;

        Ok(Box::new(rows.map(|row| row.map_err(Into::into))))
    }
}

impl StoreMut for RedbStorage {
    fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        self.0.insert_schema(schema).map_err(Into::into)
    }

    fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.0.delete_schema(table_name).map_err(Into::into)
    }

    fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        self.0.append_data(table_name, rows).map_err(Into::into)
    }

    fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> Result<()> {
        self.0.insert_data(table_name, rows).map_err(Into::into)
    }

    fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        self.0.delete_data(table_name, keys).map_err(Into::into)
    }
}

impl Transaction for RedbStorage {
    fn begin(&mut self, autocommit: bool) -> Result<bool> {
        self.0.begin(autocommit).map_err(Into::into)
    }

    fn rollback(&mut self) -> Result<()> {
        self.0.rollback().map_err(Into::into)
    }

    fn commit(&mut self) -> Result<()> {
        self.0.commit().map_err(Into::into)
    }
}

impl AlterTable for RedbStorage {}
impl Metadata for RedbStorage {}
impl CustomFunction for RedbStorage {}
impl CustomFunctionMut for RedbStorage {}
