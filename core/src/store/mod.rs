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
    if #[cfg(feature = "metadata")] {
        mod metadata;
        pub use metadata::Metadata;
    }

}

cfg_if! {
    if #[cfg(all(feature = "metadata", feature = "index"))] {
        pub trait GStore: Store + Metadata + Index {}
    } else if #[cfg(feature = "metadata")] {
        pub trait GStore: Store + Metadata {}
    } else if #[cfg(feature = "index")] {
        pub trait GStore: Store + Index {}
    } else {
        pub trait GStore: Store {}
    }
}

cfg_if! {
    if #[cfg(all(feature = "alter-table", feature = "index", feature = "transaction"))] {
        pub trait GStoreMut: StoreMut + IndexMut + AlterTable + Transaction {}
    } else if #[cfg(all(feature = "alter-table", feature = "index"))] {
        pub trait GStoreMut: StoreMut + IndexMut + AlterTable {}
    } else if #[cfg(all(feature = "alter-table", feature = "transaction"))] {
        pub trait GStoreMut: StoreMut + Transaction + AlterTable {}
    } else if #[cfg(all(feature = "index", feature = "transaction"))] {
        pub trait GStoreMut: StoreMut + IndexMut + Transaction {}
    } else if #[cfg(feature = "alter-table")] {
        pub trait GStoreMut: StoreMut + AlterTable {}
    } else if #[cfg(feature = "index")] {
        pub trait GStoreMut: StoreMut + IndexMut {}
    } else if #[cfg(feature = "transaction")] {
        pub trait GStoreMut: StoreMut + Transaction {}
    } else {
        pub trait GStoreMut: StoreMut {}
    }
}

use {
    crate::{
        data::{Key, Row, Schema},
        result::{MutResult, Result},
    },
    async_trait::async_trait,
};

pub type RowIter = Box<dyn Iterator<Item = Result<(Key, Row)>>>;

/// By implementing `Store` trait, you can run `SELECT` query.
#[async_trait(?Send)]
pub trait Store {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Row>>;

    async fn scan_data(&self, table_name: &str) -> Result<RowIter>;
}

/// By implementing `StoreMut` trait,
/// you can run `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE` and `DROP TABLE` queries.
#[async_trait(?Send)]
pub trait StoreMut
where
    Self: Sized,
{
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()>;

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()>;

    async fn append_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()>;

    async fn insert_data(self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()>;

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()>;
}
