mod alter_table;
mod data_row;
mod function;
mod index;
mod metadata;
mod transaction;

pub trait GStore: Store + Index + Metadata + CustomFunction {}
impl<S: Store + Index + Metadata + CustomFunction> GStore for S {}

pub trait GStoreMut:
    StoreMut + IndexMut + AlterTable + Transaction + CustomFunction + CustomFunctionMut + ForeignKeyMut
{
}
impl<
        S: StoreMut
            + IndexMut
            + AlterTable
            + Transaction
            + CustomFunction
            + CustomFunctionMut
            + ForeignKeyMut,
    > GStoreMut for S
{
}

pub use {
    alter_table::{AlterTable, AlterTableError},
    data_row::DataRow,
    function::{CustomFunction, CustomFunctionMut},
    index::{Index, IndexError, IndexMut},
    metadata::{MetaIter, Metadata},
    transaction::Transaction,
};

use {
    crate::{
        ast::ForeignKey,
        data::{Key, Schema},
        result::{Error, Result},
    },
    async_trait::async_trait,
};

pub type RowIter = Box<dyn Iterator<Item = Result<(Key, DataRow)>>>;

/// By implementing `Store` trait, you can run `SELECT` query.
#[async_trait(?Send)]
pub trait Store {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>>;

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>>;

    async fn scan_data(&self, table_name: &str) -> Result<RowIter>;
}

/// By implementing `StoreMut` trait,
/// you can run `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE` and `DROP TABLE` queries.
#[async_trait(?Send)]
pub trait StoreMut {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()>;

    async fn delete_schema(&mut self, table_name: &str) -> Result<()>;

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()>;

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()>;

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()>;
}

#[async_trait(?Send)]
pub trait ForeignKeyMut {
    // async fn validate(&self, table_name: &str, data: &DataRow) -> Result<()>;
    async fn add_foreign_key(&mut self, table_name: &str, foreign_key: &ForeignKey) -> Result<()> {
        let msg = "[Storage] AlterTable::add_foreign_key is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }
    async fn drop_foreign_key(
        &mut self,
        table_name: &str,
        foreign_key_name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        let msg = "[Storage] AlterTable::drop_foreign_key is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }
    async fn rename_constraint(
        &mut self,
        table_name: &str,
        old_constraint_name: &str,
        new_constraint_name: &str,
    ) -> Result<()> {
        let msg = "[Storage] AlterTable::rename_constraint is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }
}
