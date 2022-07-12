use {
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Row, Schema},
        result::{MutResult, Result},
        store::{GStore, GStoreMut, RowIter, Store, StoreMut},
        ast::ColumnOption
    },
    indexmap::IndexMap,
    rexie::{ObjectStore, RexieBuilder},
    serde::{Deserialize, Serialize},
    std::collections::HashMap,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub schema: Schema,
    pub rows: IndexMap<Key, Row>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct IndexeddbStorage {
    pub items: HashMap<String, Item>,
    // pub db: RexieBuilder,
}

#[async_trait(?Send)]
impl Store for IndexeddbStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        todo!()
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        todo!()
    }
}

impl IndexeddbStorage {
    pub async fn insert_schema(&mut self, schema: &Schema) {
        let builder = RexieBuilder::new("DB Name");

        let primary_key = schema
            .column_defs
            .iter()
            .find(|col| {
                col.options
                    .iter()
                    .any(|def| def.option == ColumnOption::Unique { is_primary: true })
            })
            .unwrap();

        let result = builder
            .version(1)
            .add_object_store(ObjectStore::new(&schema.table_name).key_path(&primary_key.name))
            .build();
        // .await?;
    }

    pub fn delete_schema(&mut self, table_name: &str) {
        todo!()
    }

    pub fn insert_data(&mut self, table_name: &str, rows: Vec<Row>) {
        todo!()
    }

    pub fn update_data(&mut self, table_name: &str, rows: Vec<(Key, Row)>) {
        todo!()
    }

    pub fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) {
        todo!()
    }
}

#[async_trait(?Send)]
impl StoreMut for IndexeddbStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        todo!()
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        todo!()
    }

    async fn insert_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        todo!()
    }

    async fn update_data(self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        todo!()
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        todo!()
    }
}

impl GStore for IndexeddbStorage {}
impl GStoreMut for IndexeddbStorage {}
