use {crate::result::TrySelf, cfg_if::cfg_if};

cfg_if! {
    if #[cfg(feature = "alter-table")] {
        mod alter_table;
        pub use alter_table::{AlterTable, AlterTableError};
    }
}

cfg_if! {
    if #[cfg(feature = "index")] {
        mod index;
        pub use index::{Index, IndexError, IndexMut};
    }
}

cfg_if! {
    if #[cfg(feature = "transaction")] {
        mod transaction;
        pub use transaction::Transaction;
    }
}

cfg_if! {
    if #[cfg(feature = "index")] {
        pub trait GStore: Store + Index {}
        impl<S: Store + Index> GStore for S {}
    } else {
        pub trait GStore: Store {}
        impl<S: Store> GStore for S {}
    }
}

cfg_if! {
    if #[cfg(all(feature = "alter-table", feature = "index", feature = "transaction"))] {
        pub trait GStoreMut: IStoreMut + IndexMut + AlterTable + Transaction {}
        impl<S: IStoreMut + IndexMut + AlterTable+ Transaction> GStoreMut for S {}
    } else if #[cfg(all(feature = "alter-table", feature = "index"))] {
        pub trait GStoreMut: IStoreMut + IndexMut + AlterTable {}
        impl<S: IStoreMut + IndexMut + AlterTable> GStoreMut for S {}
    } else if #[cfg(all(feature = "alter-table", feature = "transaction"))] {
        pub trait GStoreMut: IStoreMut + Transaction + AlterTable {}
        impl<S: IStoreMut + Transaction + AlterTable> GStoreMut for S {}
    } else if #[cfg(all(feature = "index", feature = "transaction"))] {
        pub trait GStoreMut: IStoreMut + IndexMut + Transaction {}
        impl<S: IStoreMut + IndexMut + Transaction> GStoreMut for S {}
    } else if #[cfg(feature = "alter-table")] {
        pub trait GStoreMut: IStoreMut + AlterTable {}
        impl<S: IStoreMut + AlterTable> GStoreMut for S {}
    } else if #[cfg(feature = "index")] {
        pub trait GStoreMut: IStoreMut + IndexMut {}
        impl<S: IStoreMut + IndexMut> GStoreMut for S {}
    } else if #[cfg(feature = "transaction")] {
        pub trait GStoreMut: IStoreMut + Transaction {}
        impl<S: IStoreMut + Transaction> GStoreMut for S {}
    } else {
        pub trait GStoreMut: IStoreMut {}
        impl<S: IStoreMut> GStoreMut for S {}
    }
}

mod data_row;
pub use data_row::DataRow;

use {
    crate::{
        data::{Key, Schema},
        result::{MutResult, Result},
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
pub trait IStoreMut: StoreMut
where
    Self: Sized,
{
    async fn insert_schema(mut self, schema: &Schema) -> MutResult<Self, ()>;

    async fn delete_schema(mut self, table_name: &str) -> MutResult<Self, ()>;

    async fn append_data(mut self, table_name: &str, rows: Vec<DataRow>) -> MutResult<Self, ()>;

    async fn insert_data(
        mut self,
        table_name: &str,
        rows: Vec<(Key, DataRow)>,
    ) -> MutResult<Self, ()>;

    async fn delete_data(mut self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()>;
}

#[async_trait(?Send)]
impl<T: StoreMut> IStoreMut for T {
    async fn insert_schema(mut self, schema: &Schema) -> MutResult<Self, ()> {
        StoreMut::insert_schema(&mut self, schema)
            .await
            .try_self(self)
    }

    async fn delete_schema(mut self, table_name: &str) -> MutResult<Self, ()> {
        StoreMut::delete_schema(&mut self, table_name)
            .await
            .try_self(self)
    }

    async fn append_data(mut self, table_name: &str, rows: Vec<DataRow>) -> MutResult<Self, ()> {
        StoreMut::append_data(&mut self, table_name, rows)
            .await
            .try_self(self)
    }

    async fn insert_data(
        mut self,
        table_name: &str,
        rows: Vec<(Key, DataRow)>,
    ) -> MutResult<Self, ()> {
        StoreMut::insert_data(&mut self, table_name, rows)
            .await
            .try_self(self)
    }

    async fn delete_data(mut self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        StoreMut::delete_data(&mut self, table_name, keys)
            .await
            .try_self(self)
    }
}
