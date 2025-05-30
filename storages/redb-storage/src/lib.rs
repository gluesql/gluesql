#![deny(clippy::str_to_string)]

mod core;
mod error;

use {
    async_trait::async_trait,
    core::StorageCore,
    futures::stream::iter,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, Metadata,
            RowIter, Store, StoreMut, Transaction,
        },
    },
    std::path::Path,
};

pub struct RedbStorage(StorageCore);

impl RedbStorage {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        StorageCore::new(filename).map(Self).map_err(Into::into)
    }
}

#[async_trait(?Send)]
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
        let rows = self.0.scan_data(table_name)?;
        let rows = rows.into_iter().map(Ok);

        Ok(Box::pin(iter(rows)))
    }
}

#[async_trait(?Send)]
impl StoreMut for RedbStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        self.0.insert_schema(schema).await.map_err(Into::into)
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.0.delete_schema(table_name).await.map_err(Into::into)
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        self.0
            .append_data(table_name, rows)
            .await
            .map_err(Into::into)
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        self.0
            .insert_data(table_name, rows)
            .await
            .map_err(Into::into)
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        self.0
            .delete_data(table_name, keys)
            .await
            .map_err(Into::into)
    }
}

#[async_trait(?Send)]
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
