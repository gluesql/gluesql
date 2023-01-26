#![cfg(test)]

#[cfg(feature = "alter-table")]
use crate::store::AlterTable;
#[cfg(feature = "transaction")]
use crate::store::Transaction;
#[cfg(feature = "index")]
use crate::store::{Index, IndexMut};
use {
    crate::{
        data::{Key, Schema},
        executor::execute,
        parse_sql::parse,
        result::{Error, Result},
        store::{DataRow, RowIter, Store, StoreMut},
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
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let msg = "[Storage] fetch_all_schemas not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

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

    async fn fetch_data(&self, _table_name: &str, _key: &Key) -> Result<Option<DataRow>> {
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
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let table_name = schema.table_name.clone();
        let schema = schema.clone();

        self.schema_map.insert(table_name, schema);
        Ok(())
    }

    async fn delete_schema(&mut self, _table_name: &str) -> Result<()> {
        let msg = "[MockStorage] delete_schema is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    async fn append_data(&mut self, _table_name: &str, _rows: Vec<DataRow>) -> Result<()> {
        let msg = "[MockStorage] append_data is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    async fn insert_data(&mut self, _table_name: &str, _rows: Vec<(Key, DataRow)>) -> Result<()> {
        let msg = "[MockStorage] insert_data is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    async fn delete_data(&mut self, _table_name: &str, _keys: Vec<Key>) -> Result<()> {
        let msg = "[MockStorage] delete_data is not supported".to_owned();

        Err(Error::StorageMsg(msg))
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

#[cfg(test)]
mod tests {
    #[cfg(any(feature = "alter-table", feature = "index"))]
    use crate::ast::DataType;
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
            data::Key,
            store::{Store, StoreMut},
        },
        futures::executor::block_on,
    };

    #[cfg(any(feature = "alter-table", feature = "index", feature = "transaction"))]
    fn test<T, F>(result: F) -> MockStorage
    where
        F: std::future::Future<Output = crate::result::MutResult<MockStorage, T>>,
    {
        match block_on(result) {
            Ok(_) => unreachable!("this test must fail"),
            Err((storage, _)) => storage,
        }
    }

    #[test]
    fn empty() {
        let mut storage = MockStorage::default();

        assert!(block_on(storage.scan_data("Foo")).is_err());
        assert!(block_on(storage.fetch_data("Foo", &Key::None)).is_err());
        assert!(block_on(storage.fetch_schema("__Err__")).is_err());
        assert!(block_on(storage.delete_schema("Foo")).is_err());
        assert!(block_on(storage.append_data("Foo", Vec::new())).is_err());
        assert!(block_on(storage.insert_data("Foo", Vec::new())).is_err());
        assert!(block_on(storage.delete_data("Foo", Vec::new())).is_err());

        #[cfg(feature = "alter-table")]
        let storage = {
            let storage = test(storage.rename_schema("Foo", "Bar"));
            let storage = test(storage.rename_column("Foo", "col_old", "col_new"));
            let storage = test(storage.add_column(
                "Foo",
                &ColumnDef {
                    name: "new_col".to_owned(),
                    data_type: DataType::Boolean,
                    nullable: false,
                    default: None,
                    unique: None,
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

        assert!(matches!(block_on(storage.fetch_schema("Foo")), Ok(None)));
    }
}
