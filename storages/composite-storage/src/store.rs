use {
    super::CompositeStorage,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result,
        store::{RowIter, Store},
    },
};

fn with_fallback_engine(mut schema: Schema, engine: &str) -> Schema {
    schema.engine.get_or_insert_with(|| engine.to_owned());
    schema
}

impl Store for CompositeStorage {
    fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut schemas = Vec::new();

        for (engine, storage) in &self.storages {
            schemas.extend(
                storage
                    .fetch_all_schemas()?
                    .into_iter()
                    .map(|schema| with_fallback_engine(schema, engine)),
            );
        }

        Ok(schemas)
    }

    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        for (engine, storage) in &self.storages {
            if let Some(schema) = storage.fetch_schema(table_name)? {
                return Ok(Some(with_fallback_engine(schema, engine)));
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
