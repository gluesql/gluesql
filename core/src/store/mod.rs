mod alter_table;
mod data_row;
mod function;
mod index;
mod metadata;
mod transaction;

pub trait GStore: Store + Index + Metadata + CustomFunction {}
impl<S: Store + Index + Metadata + CustomFunction> GStore for S {}

pub trait GStoreMut:
    StoreMut + IndexMut + AlterTable + Transaction + CustomFunction + CustomFunctionMut
{
}
impl<S: StoreMut + IndexMut + AlterTable + Transaction + CustomFunction + CustomFunctionMut>
    GStoreMut for S
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
        data::{Key, Schema},
        executor::Referencing,
        result::{Error, Result},
    },
    async_trait::async_trait,
    futures::stream::Stream,
    std::pin::Pin,
};

pub type RowIter<'a> = Pin<Box<dyn Stream<Item = Result<(Key, DataRow)>> + 'a>>;

/// By implementing `Store` trait, you can run `SELECT` query.
#[async_trait(?Send)]
pub trait Store {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>>;

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>>;

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>>;

    async fn fetch_referencings(&self, table_name: &str) -> Result<Vec<Referencing>> {
        let schemas = self.fetch_all_schemas().await?;

        Ok(schemas
            .into_iter()
            .flat_map(|schema| {
                let Schema {
                    table_name: referencing_table_name,
                    foreign_keys,
                    ..
                } = schema;

                foreign_keys.into_iter().filter_map(move |foreign_key| {
                    (foreign_key.referenced_table_name == table_name
                        && referencing_table_name != table_name)
                        .then_some(Referencing {
                            table_name: referencing_table_name.clone(),
                            foreign_key,
                        })
                })
            })
            .collect())
    }
}

/// By implementing `StoreMut` trait,
/// you can run `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE` and `DROP TABLE` queries.
#[async_trait(?Send)]
pub trait StoreMut {
    async fn insert_schema(&mut self, _schema: &Schema) -> Result<()> {
        let msg = "[Storage] StoreMut::insert_schema is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    async fn delete_schema(&mut self, _table_name: &str) -> Result<()> {
        let msg = "[Storage] StoreMut::delete_schema is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    async fn append_data(&mut self, _table_name: &str, _rows: Vec<DataRow>) -> Result<()> {
        let msg = "[Storage] StoreMut::append_data is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    async fn insert_data(&mut self, _table_name: &str, _rows: Vec<(Key, DataRow)>) -> Result<()> {
        let msg = "[Storage] StoreMut::insert_data is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    async fn delete_data(&mut self, _table_name: &str, _keys: Vec<Key>) -> Result<()> {
        let msg = "[Storage] StoreMut::delete_data is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }
}
