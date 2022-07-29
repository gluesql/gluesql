#![cfg(test)]

#[cfg(feature = "alter-table")]
use crate::store::AlterTable;
#[cfg(feature = "metadata")]
use crate::store::Metadata;
#[cfg(feature = "transaction")]
use crate::store::Transaction;
#[cfg(feature = "index")]
use crate::store::{Index, IndexMut};
use {
    crate::{
        data::{Key, Row, Schema},
        executor::execute,
        parse_sql::parse,
        result::{Error, MutResult, Result},
        store::{GStore, GStoreMut, RowIter, Store, StoreMut},
        translate::translate,
    },
    async_trait::async_trait,
    futures::{
        executor::block_on,
        stream::{self, StreamExt},
    },
    std::collections::HashMap,
};

pub fn run(sql: &str) -> MockStorage {
    let storage =
        stream::iter(parse(sql).unwrap()).fold(MockStorage::default(), |storage, parsed| {
            let statement = translate(&parsed).unwrap();

            async move {
                let (storage, _) = execute(storage, &statement).await.unwrap();

                storage
            }
        });

    block_on(storage)
}

#[derive(Default, Debug)]
pub struct MockStorage {
    schema_map: HashMap<String, Schema>,
}

#[async_trait(?Send)]
impl Store for MockStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        if table_name == "__Err__" {
            return Err(Error::StorageMsg(
                "[MockStorage] fetch_schema - user triggered error".to_owned(),
            ));
        }

        self.schema_map
            .get(table_name)
            .map(|schema| Ok(schema.clone()))
            .transpose()
    }

    async fn fetch_data(&self, _table_name: &str, _key: &Key) -> Result<Option<Row>> {
        Err(Error::StorageMsg(
            "[MockStorage] fetch_data not supported".to_owned(),
        ))
    }

    async fn scan_data(&self, _table_name: &str) -> Result<RowIter> {
        Err(Error::StorageMsg(
            "[MockStorage] scan_data not supported".to_owned(),
        ))
    }
}

#[async_trait(?Send)]
impl StoreMut for MockStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let mut storage = self;

        let table_name = schema.table_name.clone();
        let schema = schema.clone();

        storage.schema_map.insert(table_name, schema);
        Ok((storage, ()))
    }

    async fn delete_schema(self, _table_name: &str) -> MutResult<Self, ()> {
        let msg = "[MockStorage] delete_schema is not supported".to_owned();

        Err((self, Error::StorageMsg(msg)))
    }

    async fn insert_data(self, _table_name: &str, _rows: Vec<Row>) -> MutResult<Self, ()> {
        let msg = "[MockStorage] insert_data is not supported".to_owned();

        Err((self, Error::StorageMsg(msg)))
    }

    async fn update_data(self, _table_name: &str, _rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        let msg = "[MockStorage] update_data is not supported".to_owned();

        Err((self, Error::StorageMsg(msg)))
    }

    async fn delete_data(self, _table_name: &str, _keys: Vec<Key>) -> MutResult<Self, ()> {
        let msg = "[MockStorage] delete_data is not supported".to_owned();

        Err((self, Error::StorageMsg(msg)))
    }
}

#[cfg(feature = "alter-table")]
impl AlterTable for MockStorage {}

#[cfg(feature = "index")]
impl Index for MockStorage {}

#[cfg(feature = "index")]
impl IndexMut for MockStorage {}

#[cfg(feature = "transaction")]
impl Transaction for MockStorage {}

#[cfg(feature = "metadata")]
impl Metadata for MockStorage {}

impl GStore for MockStorage {}
impl GStoreMut for MockStorage {}

#[cfg(test)]
mod tests {
    #[cfg(any(feature = "alter-table", feature = "index"))]
    use crate::ast::DataType;
    #[cfg(feature = "metadata")]
    use crate::store::Metadata;
    #[cfg(feature = "transaction")]
    use crate::store::Transaction;
    #[cfg(feature = "alter-table")]
    use crate::{ast::ColumnDef, store::AlterTable};
    #[cfg(feature = "index")]
    use crate::{
        ast::{Expr, OrderByExpr},
        store::{Index, IndexMut},
    };
    use {
        super::MockStorage,
        crate::{
            result::MutResult,
            data::Key,
            store::{Store, StoreMut},
        },
        futures::executor::block_on,
        std::future::Future,
    };

    fn test<T, F>(result: F) -> MockStorage
    where
        F: Future<Output = MutResult<MockStorage, T>>,
    {
        match block_on(result) {
            Ok(_) => unreachable!("this test must fail"),
            Err((storage, _)) => storage,
        }
    }

    #[test]
    fn empty() {
        let storage = MockStorage::default();

        assert!(block_on(storage.scan_data("Foo")).is_err());
        assert!(block_on(storage.fetch_data("Foo", &Key::None)).is_err());
        assert!(block_on(storage.fetch_schema("__Err__")).is_err());
        let storage = test(storage.delete_schema("Foo"));
        let storage = test(storage.insert_data("Foo", Vec::new()));
        let storage = test(storage.update_data("Foo", Vec::new()));
        let storage = test(storage.delete_data("Foo", Vec::new()));

        #[cfg(feature = "alter-table")]
        let storage = {
            let storage = test(storage.rename_schema("Foo", "Bar"));
            let storage = test(storage.rename_column("Foo", "col_old", "col_new"));
            let storage = test(storage.add_column(
                "Foo",
                &ColumnDef {
                    name: "new_col".to_owned(),
                    data_type: DataType::Boolean,
                    options: Vec::new(),
                },
            ));
            let storage = test(storage.drop_column("Foo", "col", false));

            storage
        };

        #[cfg(feature = "index")]
        let storage = {
            assert!(block_on(storage.scan_indexed_data("Foo", "idx_col", None, None)).is_err());
            let storage = test(storage.create_index(
                "Foo",
                "idx_col",
                &OrderByExpr {
                    expr: Expr::TypedString {
                        data_type: DataType::Boolean,
                        value: "true".to_owned(),
                    },
                    asc: None,
                },
            ));
            let storage = test(storage.drop_index("Foo", "idx_col"));

            storage
        };

        #[cfg(feature = "transaction")]
        let storage = {
            let storage = test(storage.begin(false));
            let storage = test(storage.rollback());
            let storage = test(storage.commit());

            storage
        };

        #[cfg(feature = "metadata")]
        {
            storage.version();
            assert!(block_on(storage.schema_names()).is_err());
        };

        assert!(matches!(block_on(storage.fetch_schema("Foo")), Ok(None)));
    }
}
