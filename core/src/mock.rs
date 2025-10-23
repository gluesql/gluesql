use {
    crate::{
        ast::OrderByExpr,
        data::{Key, Schema, SchemaIndex, SchemaIndexOrd},
        result::{Error, Result},
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, Metadata,
            RowIter, Store, StoreMut, Transaction,
        },
    },
    async_trait::async_trait,
    std::collections::HashMap,
};

#[cfg(test)]
use {
    crate::{executor::execute, parse_sql::parse, translate::translate},
    futures::executor::block_on,
};

#[cfg(test)]
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

#[async_trait]
impl CustomFunction for MockStorage {}

#[async_trait]
impl CustomFunctionMut for MockStorage {}

#[async_trait]
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

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let msg = "[Storage] fetch_all_schemas not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    async fn fetch_data(&self, _table_name: &str, _key: &Key) -> Result<Option<DataRow>> {
        Err(Error::StorageMsg(
            "[MockStorage] fetch_data not supported".to_owned(),
        ))
    }

    async fn scan_data<'a>(&'a self, _table_name: &str) -> Result<RowIter<'a>> {
        Err(Error::StorageMsg(
            "[MockStorage] scan_data not supported".to_owned(),
        ))
    }
}

#[async_trait]
impl StoreMut for MockStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let table_name = schema.table_name.clone();
        let schema = schema.clone();

        self.schema_map.insert(table_name, schema);
        Ok(())
    }
}

impl AlterTable for MockStorage {}
impl Index for MockStorage {}
#[async_trait]
impl IndexMut for MockStorage {
    async fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &OrderByExpr,
    ) -> Result<()> {
        use chrono::Utc;

        let schema = self.schema_map.get_mut(table_name).ok_or_else(|| {
            Error::StorageMsg(format!(
                "[MockStorage] create_index - table not found: {table_name}"
            ))
        })?;

        if schema.indexes.iter().any(|index| index.name == index_name) {
            return Err(Error::StorageMsg(format!(
                "[MockStorage] create_index - index already exists: {index_name}"
            )));
        }

        let order = match column.asc {
            Some(true) => SchemaIndexOrd::Asc,
            Some(false) => SchemaIndexOrd::Desc,
            None => SchemaIndexOrd::Both,
        };

        let expr = column.expr.clone();
        let created = Utc::now().naive_utc();

        schema.indexes.push(SchemaIndex {
            name: index_name.to_owned(),
            expr,
            order,
            created,
        });

        Ok(())
    }
}
impl Transaction for MockStorage {}
impl Metadata for MockStorage {}

#[cfg(test)]
mod tests {
    use {
        super::MockStorage,
        crate::{
            ast::{ColumnDef, DataType, Expr, OrderByExpr},
            data::{Key, Schema, SchemaIndexOrd},
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
        assert!(block_on(storage.fetch_all_schemas()).is_err());
        assert!(block_on(storage.delete_schema("Foo")).is_err());
        assert!(block_on(storage.append_data("Foo", Vec::new())).is_err());
        assert!(block_on(storage.insert_data("Foo", Vec::new())).is_err());
        assert!(block_on(storage.delete_data("Foo", Vec::new())).is_err());

        // AlterTable
        assert!(block_on(storage.rename_schema("Foo", "Bar")).is_err());
        assert!(block_on(storage.rename_column("Foo", "col_old", "col_new")).is_err());
        assert!(
            block_on(storage.add_column(
                "Foo",
                &ColumnDef {
                    name: "new_col".to_owned(),
                    data_type: DataType::Boolean,
                    nullable: false,
                    default: None,
                    unique: None,
                    comment: None,
                },
            ))
            .is_err()
        );
        assert!(block_on(storage.drop_column("Foo", "col", false)).is_err());

        // Index & IndexMut
        assert!(block_on(storage.scan_indexed_data("Foo", "idx_col", None, None)).is_err());
        assert!(
            block_on(storage.create_index(
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
            .is_err()
        );
        assert!(block_on(storage.drop_index("Foo", "idx_col")).is_err());

        // Transaction
        assert!(block_on(storage.begin(false)).is_err());
        assert!(block_on(storage.rollback()).is_ok());
        assert!(block_on(storage.commit()).is_ok());

        assert!(matches!(block_on(storage.fetch_schema("Foo")), Ok(None)));
    }

    #[test]
    fn create_index_adds_schema_entry() {
        let mut storage = MockStorage::default();

        let schema = Schema {
            table_name: "Test".to_owned(),
            column_defs: None,
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        };
        block_on(storage.insert_schema(&schema)).unwrap();

        let order_by = OrderByExpr {
            expr: Expr::Identifier("id".to_owned()),
            asc: Some(true),
        };
        let expected_expr = order_by.expr.clone();

        block_on(storage.create_index("Test", "idx_id", &order_by)).unwrap();

        let schema = block_on(storage.fetch_schema("Test"))
            .unwrap()
            .expect("schema should exist");
        assert_eq!(schema.indexes.len(), 1);

        let index = &schema.indexes[0];
        assert_eq!(index.name, "idx_id");
        assert_eq!(index.expr, expected_expr);
        assert_eq!(index.order, SchemaIndexOrd::Asc);
    }

    #[test]
    fn create_index_checks_duplicates_and_orders() {
        let mut storage = MockStorage::default();

        let schema = Schema {
            table_name: "Test".to_owned(),
            column_defs: None,
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        };
        block_on(storage.insert_schema(&schema)).unwrap();

        let desc_order = OrderByExpr {
            expr: Expr::Identifier("value".to_owned()),
            asc: Some(false),
        };

        block_on(storage.create_index("Test", "idx_desc", &desc_order)).unwrap();

        let schema = block_on(storage.fetch_schema("Test"))
            .unwrap()
            .expect("schema should exist");
        assert_eq!(schema.indexes.len(), 1);
        let index = &schema.indexes[0];
        assert_eq!(index.name, "idx_desc");
        assert_eq!(index.order, SchemaIndexOrd::Desc);

        let duplicate_err = block_on(storage.create_index("Test", "idx_desc", &desc_order))
            .expect_err("duplicate index creation should fail");
        let msg = format!("{duplicate_err}");
        assert!(
            msg.contains("index already exists: idx_desc"),
            "unexpected error message: {msg}"
        );
    }
}
