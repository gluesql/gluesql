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
        pub trait GStore<T: Debug>: Store<T> + Index<T> {}
    } else {
        pub trait GStore<T: Debug>: Store<T> {}
    }
}

cfg_if! {
    if #[cfg(all(feature = "alter-table", feature = "index", feature = "transaction"))] {
        pub trait GStoreMut<T: Debug>: StoreMut<T> + IndexMut<T> + AlterTable + Transaction {}
    } else if #[cfg(all(feature = "alter-table", feature = "index"))] {
        pub trait GStoreMut<T: Debug>: StoreMut<T> + IndexMut<T> + AlterTable {}
    } else if #[cfg(all(feature = "alter-table", feature = "transaction"))] {
        pub trait GStoreMut<T: Debug>: StoreMut<T> + Transaction + AlterTable {}
    } else if #[cfg(all(feature = "index", feature = "transaction"))] {
        pub trait GStoreMut<T: Debug>: StoreMut<T> + IndexMut<T> + Transaction {}
    } else if #[cfg(feature = "alter-table")] {
        pub trait GStoreMut<T: Debug>: StoreMut<T> + AlterTable {}
    } else if #[cfg(feature = "index")] {
        pub trait GStoreMut<T: Debug>: StoreMut<T> + IndexMut<T> {}
    } else if #[cfg(feature = "transaction")] {
        pub trait GStoreMut<T: Debug>: StoreMut<T> + Transaction {}
    } else {
        pub trait GStoreMut<T: Debug>: Store<T> + StoreMut<T> {}
    }
}

use {
    crate::{
        data::{Row, Schema},
        result::{MutResult, Result},
    },
    async_trait::async_trait,
    std::fmt::Debug,
};

pub type RowIter<T> = Box<dyn Iterator<Item = Result<(T, Row)>>>;

/// By implementing `Store` trait, you can run `SELECT` query.
#[async_trait(?Send)]
pub trait Store<T: Debug> {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<T>>;
}

/// By implementing `StoreMut` trait,
/// you can run `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE` and `DROP TABLE` queries.
#[async_trait(?Send)]
pub trait StoreMut<T: Debug>
where
    Self: Sized,
{
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()>;

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()>;

    async fn insert_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()>;

    async fn update_data(self, table_name: &str, rows: Vec<(T, Row)>) -> MutResult<Self, ()>;

    async fn delete_data(self, table_name: &str, keys: Vec<T>) -> MutResult<Self, ()>;
}
