#![cfg(test)]

#[cfg(feature = "function")]
use crate::store::Function;

use {
    crate::{
        data::{Key, Schema},
        executor::execute,
        parse_sql::parse,
        result::{Error, Result},
        store::{AlterTable, DataRow, Index, IndexMut, RowIter, Store, StoreMut, Transaction},
        translate::translate,
    },
    async_trait::async_trait,
    futures::executor::block_on,
    std::collections::HashMap,
};

pub fn run(sql: &str) -> MockStorage {
    let mut storage = MockStorage::default();

    for parsed in parse(sql).unwrap() {
        let statement = translate(&parsed).unwrap();

        block_on(execute(&mut storage, &statement)).unwrap();
    }

    storage
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

impl AlterTable for MockStorage {}
impl Index for MockStorage {}
impl IndexMut for MockStorage {}
impl Transaction for MockStorage {}

#[cfg(feature = "function")]
impl Function for MockStorage {}

#[cfg(test)]
mod tests {
    use {
        super::MockStorage,
        crate::{
            ast::{ColumnDef, DataType, Expr, OrderByExpr},
            data::Key,
            store::{AlterTable, Index, IndexMut, Transaction},
            store::{Store, StoreMut},
        },
        futures::executor::block_on,
    };

    #[test]
    fn empty() {
        let mut storage = MockStorage::default();

        // Store & StoreMut
        assert!(block_on(storage.scan_data("Foo")).is_err());
        assert!(block_on(storage.fetch_data("Foo", &Key::None)).is_err());
        assert!(block_on(storage.fetch_schema("__Err__")).is_err());
        assert!(block_on(storage.delete_schema("Foo")).is_err());
        assert!(block_on(storage.append_data("Foo", Vec::new())).is_err());
        assert!(block_on(storage.insert_data("Foo", Vec::new())).is_err());
        assert!(block_on(storage.delete_data("Foo", Vec::new())).is_err());

        // AlterTable
        assert!(block_on(storage.rename_schema("Foo", "Bar")).is_err());
        assert!(block_on(storage.rename_column("Foo", "col_old", "col_new")).is_err());
        assert!(block_on(storage.add_column(
            "Foo",
            &ColumnDef {
                name: "new_col".to_owned(),
                data_type: DataType::Boolean,
                nullable: false,
                default: None,
                unique: None,
            },
        ))
        .is_err());
        assert!(block_on(storage.drop_column("Foo", "col", false)).is_err());

        // Index & IndexMut
        assert!(block_on(storage.scan_indexed_data("Foo", "idx_col", None, None)).is_err());
        assert!(block_on(storage.create_index(
            "Foo",
            "idx_col",
            &OrderByExpr {
                expr: Expr::TypedString {
                    data_type: DataType::Boolean,
                    value: "true".to_owned(),
                },
                asc: None,
            },
        ))
        .is_err());
        assert!(block_on(storage.drop_index("Foo", "idx_col")).is_err());

        // Transaction
        assert!(block_on(storage.begin(false)).is_err());
        assert!(block_on(storage.rollback()).is_ok());
        assert!(block_on(storage.commit()).is_ok());

        assert!(matches!(block_on(storage.fetch_schema("Foo")), Ok(None)));
    }
}
