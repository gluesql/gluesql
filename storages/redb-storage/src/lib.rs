#![deny(clippy::str_to_string)]

mod core;
mod error;
mod migration;
mod planner;

#[cfg(test)]
mod bincode_layout;

pub use migration::{MigrationReport, REDB_STORAGE_FORMAT_VERSION, migrate_to_latest};

use {
    core::StorageCore,
    gluesql_core::{
        ast::{ColumnDef, IndexOperator, OrderByExpr},
        data::{Key, Schema, Value},
        error::Result,
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata, RowIter,
            ScanColumn, Store, StoreMut, Transaction,
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

    fn scan_data<'a>(
        &'a self,
        table_name: &str,
        columns: Option<&[ScanColumn]>,
    ) -> Result<RowIter<'a>> {
        let rows = self.0.scan_data(table_name)?;
        let column_indexes =
            columns.map(|columns| columns.iter().map(|(_, index)| *index).collect::<Vec<_>>());

        Ok(Box::new(rows.map(move |row| {
            let (key, row) = row?;
            let row = match &column_indexes {
                Some(indexes) => indexes.iter().map(|index| row[*index].clone()).collect(),
                None => row,
            };

            Ok((key, row))
        })))
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

impl AlterTable for RedbStorage {
    fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()> {
        self.0
            .rename_schema(table_name, new_table_name)
            .map_err(Into::into)
    }

    fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        self.0
            .rename_column(table_name, old_column_name, new_column_name)
            .map_err(Into::into)
    }

    fn add_column(
        &mut self,
        table_name: &str,
        column_def: &ColumnDef,
        default_value: Value,
    ) -> Result<()> {
        self.0
            .add_column(table_name, column_def, &default_value)
            .map_err(Into::into)
    }

    fn drop_column(&mut self, table_name: &str, column_name: &str, if_exists: bool) -> Result<()> {
        self.0
            .drop_column(table_name, column_name, if_exists)
            .map_err(Into::into)
    }
}

impl Index for RedbStorage {
    fn scan_indexed_data<'a>(
        &'a self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter<'a>> {
        let rows = self
            .0
            .scan_indexed_data(table_name, index_name, asc, cmp_value)?;
        Ok(Box::new(rows.map(|row| row.map_err(Into::into))))
    }
}

impl IndexMut for RedbStorage {
    fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &OrderByExpr,
    ) -> Result<()> {
        self.0
            .create_index(table_name, index_name, column)
            .map_err(Into::into)
    }

    fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()> {
        self.0
            .drop_index(table_name, index_name)
            .map_err(Into::into)
    }
}
impl Metadata for RedbStorage {}
impl CustomFunction for RedbStorage {}
impl CustomFunctionMut for RedbStorage {}
