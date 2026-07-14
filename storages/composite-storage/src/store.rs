use {
    super::{CompositeStorage, IStorage},
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result,
        store::{RowIter, Store},
    },
};

impl Store for CompositeStorage {
    fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let schemas = self
            .storages
            .values()
            .map(AsRef::as_ref)
            .map(<dyn IStorage>::fetch_all_schemas)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(schemas)
    }

    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        for storage in self.storages.values() {
            let schema = storage.fetch_schema(table_name)?;

            if schema.is_some() {
                return Ok(schema);
            }
        }

        Ok(None)
    }

    fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
        self.fetch_storage(table_name)?.fetch_data(table_name, key)
    }

    fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        self.fetch_storage(table_name)?.scan_data(table_name)
    }
}
