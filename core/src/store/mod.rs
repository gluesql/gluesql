use {
    crate::{
        data::{Row, Schema},
        result::{MutResult, Result},
    },
    async_trait::async_trait,
    cfg_if::cfg_if,
};

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
        pub trait GStore<T>: Store<Key = T> + Metadata + Index<Key = T> {}
    } else if #[cfg(feature = "metadata")] {
        pub trait GStore<T>: Store<Key = T> + Metadata {}
    } else if #[cfg(feature = "index")] {
        pub trait GStore<T>: Store<Key = T> + Index<Key = T> {}
    } else {
        pub trait GStore<T>: Store<Key = T> {}
    }
}

cfg_if! {
    if #[cfg(all(feature = "alter-table", feature = "index", feature = "transaction"))] {
        pub trait GStoreMut<T>: StoreMut<Key = T> + IndexMut + AlterTable + Transaction {}
    } else if #[cfg(all(feature = "alter-table", feature = "index"))] {
        pub trait GStoreMut<T>: StoreMut<Key = T> + IndexMut + AlterTable {}
    } else if #[cfg(all(feature = "alter-table", feature = "transaction"))] {
        pub trait GStoreMut<T>: StoreMut<Key = T> + Transaction + AlterTable {}
    } else if #[cfg(all(feature = "index", feature = "transaction"))] {
        pub trait GStoreMut<T>: StoreMut<Key = T> + IndexMut + Transaction {}
    } else if #[cfg(feature = "alter-table")] {
        pub trait GStoreMut<T>: StoreMut<Key = T> + AlterTable {}
    } else if #[cfg(feature = "index")] {
        pub trait GStoreMut<T>: StoreMut<Key = T> + IndexMut {}
    } else if #[cfg(feature = "transaction")] {
        pub trait GStoreMut<T>: StoreMut<Key = T> + Transaction {}
    } else {
        pub trait GStoreMut<T>: Store<Key = T> + StoreMut<Key = T> {}
    }
}

pub type RowIter<T> = Box<dyn Iterator<Item = Result<(T, Row)>>>;

/// By implementing `Store` trait, you can run `SELECT` query.
#[async_trait(?Send)]
pub trait Store {
    type Key;

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<Self::Key>>;
}

/// By implementing `StoreMut` trait,
/// you can run `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE` and `DROP TABLE` queries.
#[async_trait(?Send)]
pub trait StoreMut
where
    Self: Sized,
{
    type Key;

    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()>;

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()>;

    async fn insert_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()>;

    async fn update_data(
        self,
        table_name: &str,
        rows: Vec<(Self::Key, Row)>,
    ) -> MutResult<Self, ()>;

    async fn delete_data(self, table_name: &str, keys: Vec<Self::Key>) -> MutResult<Self, ()>;
}
