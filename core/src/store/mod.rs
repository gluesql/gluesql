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
        pub trait GStore<T>: Store<T> + Metadata + Index<T> {}
    } else if #[cfg(feature = "metadata")] {
        pub trait GStore<T>: Store<T> + Metadata {}
    } else if #[cfg(feature = "index")] {
        pub trait GStore<T>: Store<T> + Index<T> {}
    } else {
        pub trait GStore<T>: Store<T> {}
    }
}

cfg_if! {
    if #[cfg(all(feature = "alter-table", feature = "index", feature = "transaction"))] {
        pub trait GStoreMut<T>: StoreMut<T> + IndexMut + AlterTable + Transaction {}
    } else if #[cfg(all(feature = "alter-table", feature = "index"))] {
        pub trait GStoreMut<T>: StoreMut<T> + IndexMut + AlterTable {}
    } else if #[cfg(all(feature = "alter-table", feature = "transaction"))] {
        pub trait GStoreMut<T>: StoreMut<T> + Transaction + AlterTable {}
    } else if #[cfg(all(feature = "index", feature = "transaction"))] {
        pub trait GStoreMut<T>: StoreMut<T> + IndexMut + Transaction {}
    } else if #[cfg(feature = "alter-table")] {
        pub trait GStoreMut<T>: StoreMut<T> + AlterTable {}
    } else if #[cfg(feature = "index")] {
        pub trait GStoreMut<T>: StoreMut<T> + IndexMut {}
    } else if #[cfg(feature = "transaction")] {
        pub trait GStoreMut<T>: StoreMut<T> + Transaction {}
    } else {
        pub trait GStoreMut<T>: Store<T> + StoreMut<T> {}
    }
}

use {
    crate::{
        data::{Row, Schema},
        result::{MutResult, Result},
    },
    async_trait::async_trait,
};

pub type RowIter<T> = Box<dyn Iterator<Item = Result<(T, Row)>>>;

/// By implementing `Store` trait, you can run `SELECT` query.
#[async_trait(?Send)]
pub trait Store<T> {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<T>>;
}

/// By implementing `StoreMut` trait,
/// you can run `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE` and `DROP TABLE` queries.
#[async_trait(?Send)]
pub trait StoreMut<T>
where
    Self: Sized,
{
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()>;

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()>;

    async fn insert_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()>;

    async fn update_data(self, table_name: &str, rows: Vec<(T, Row)>) -> MutResult<Self, ()>;

    async fn delete_data(self, table_name: &str, keys: Vec<T>) -> MutResult<Self, ()>;
}
