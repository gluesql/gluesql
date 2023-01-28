use cfg_if::cfg_if;

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
        pub trait GStoreMut: StoreMut + IndexMut + AlterTable + Transaction {}
        impl<S: StoreMut + IndexMut + AlterTable+ Transaction> GStoreMut for S {}
    } else if #[cfg(all(feature = "alter-table", feature = "index"))] {
        pub trait GStoreMut: StoreMut + IndexMut + AlterTable {}
        impl<S: StoreMut + IndexMut + AlterTable> GStoreMut for S {}
    } else if #[cfg(all(feature = "alter-table", feature = "transaction"))] {
        pub trait GStoreMut: StoreMut + Transaction + AlterTable {}
        impl<S: StoreMut + Transaction + AlterTable> GStoreMut for S {}
    } else if #[cfg(all(feature = "index", feature = "transaction"))] {
        pub trait GStoreMut: StoreMut + IndexMut + Transaction {}
        impl<S: StoreMut + IndexMut + Transaction> GStoreMut for S {}
    } else if #[cfg(feature = "alter-table")] {
        pub trait GStoreMut: StoreMut + AlterTable {}
        impl<S: StoreMut + AlterTable> GStoreMut for S {}
    } else if #[cfg(feature = "index")] {
        pub trait GStoreMut: StoreMut + IndexMut {}
        impl<S: StoreMut + IndexMut> GStoreMut for S {}
    } else if #[cfg(feature = "transaction")] {
        pub trait GStoreMut: StoreMut + Transaction {}
        impl<S: StoreMut + Transaction> GStoreMut for S {}
    } else {
        pub trait GStoreMut: StoreMut {}
        impl<S: StoreMut> GStoreMut for S {}
    }
}

mod data_row;
pub use data_row::DataRow;

use {
    crate::{
        data::{Key, Schema},
        result::Result,
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
    async fn insert_schema(&mut self, schema: &Schema) -> Result<&mut Self>;

    async fn delete_schema(&mut self, table_name: &str) -> Result<&mut Self>;

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<&mut Self>;

    async fn insert_data(
        &mut self,
        table_name: &str,
        rows: Vec<(Key, DataRow)>,
    ) -> Result<&mut Self>;

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<&mut Self>;
}
