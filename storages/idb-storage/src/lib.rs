#![cfg(target_arch = "wasm32")]
#![deny(clippy::str_to_string)]

pub mod convert;
mod error;

use {
    async_trait::async_trait,
    convert::convert,
    error::ErrInto,
    futures::stream::{empty, iter},
    gloo_utils::format::JsValueSerdeExt,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::{Error, Result},
        store::{DataRow, Metadata, RowIter, Store, StoreMut},
    },
    idb::{CursorDirection, Database, Factory, ObjectStoreParams, Query, TransactionMode},
    serde_json::Value as JsonValue,
    std::sync::{Arc, Mutex},
    wasm_bindgen::JsValue,
    web_sys::console,
};

const SCHEMA_STORE: &str = "gluesql-schema";
const DEFAULT_NAMESPACE: &str = "gluesql";

pub struct IdbStorage {
    namespace: String,
    factory: Factory,
    database: Database,
}

impl IdbStorage {
    pub async fn new(namespace: Option<String>) -> Result<Self> {
        let factory = Factory::new().err_into()?;
        let namespace = namespace.as_deref().unwrap_or(DEFAULT_NAMESPACE).to_owned();

        let error = Arc::new(Mutex::new(None));
        let open_request = {
            let error = Arc::clone(&error);
            let mut open_request = factory.open(namespace.as_str(), None).err_into()?;
            open_request.on_upgrade_needed(move |event| {
                let database = match event.database().err_into() {
                    Ok(database) => database,
                    Err(e) => {
                        let mut error = match error.lock() {
                            Ok(error) => error,
                            Err(_) => {
                                let msg = JsValue::from_str("infallible - lock acquire failed");
                                console::error_1(&msg);
                                return;
                            }
                        };

                        *error = Some(e);
                        return;
                    }
                };

                if let Err(e) = database
                    .create_object_store(SCHEMA_STORE, ObjectStoreParams::new())
                    .err_into()
                {
                    let mut error = match error.lock() {
                        Ok(error) => error,
                        Err(_) => {
                            let msg = JsValue::from_str("infallible - lock acquire failed");
                            console::error_1(&msg);
                            return;
                        }
                    };

                    *error = Some(e);
                }
            });

            open_request
        };

        let database = open_request.await.err_into()?;
        if let Some(e) = Arc::try_unwrap(error)
            .map_err(|_| Error::StorageMsg("infallible - Arc::try_unwrap failed".to_owned()))?
            .into_inner()
            .err_into()?
        {
            return Err(e);
        }

        Ok(Self {
            namespace,
            factory,
            database,
        })
    }

    pub async fn delete(&self) -> Result<()> {
        self.factory.delete(&self.namespace).await.err_into()
    }
}

#[async_trait(?Send)]
impl Store for IdbStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadOnly)
            .err_into()?;

        let store = transaction.object_store(SCHEMA_STORE).err_into()?;
        let schemas = store.get_all(None, None).await.err_into()?;

        transaction.commit().await.err_into()?;
        schemas
            .into_iter()
            .map(|schema| {
                schema
                    .as_string()
                    .as_deref()
                    .ok_or_else(|| {
                        Error::StorageMsg("conflict - invalid schema value: {schema:?}".to_owned())
                    })
                    .and_then(Schema::from_ddl)
            })
            .collect::<Result<Vec<Schema>>>()
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadOnly)
            .err_into()?;

        let store = transaction.object_store(SCHEMA_STORE).err_into()?;
        let schema = store
            .get(JsValue::from_str(table_name))
            .await
            .err_into()?
            .and_then(|schema| JsValue::as_string(&schema))
            .map(|schema| Schema::from_ddl(schema.as_str()))
            .transpose()?;

        transaction.commit().await.err_into()?;
        Ok(schema)
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        let column_defs = self
            .fetch_schema(table_name)
            .await?
            .and_then(|schema| schema.column_defs);
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadOnly)
            .err_into()?;

        let store = transaction.object_store(table_name).err_into()?;

        let key: Value = target.clone().into();
        let key: JsonValue = key.try_into()?;
        let key = JsValue::from_serde(&key).err_into()?;
        let row = store.get(key).await.err_into()?;

        transaction.commit().await.err_into()?;

        match row {
            Some(row) => convert(row, column_defs.as_deref()).map(Some),
            None => Ok(None),
        }
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let column_defs = self
            .fetch_schema(table_name)
            .await?
            .and_then(|schema| schema.column_defs);
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadOnly)
            .err_into()?;

        let store = transaction.object_store(table_name).err_into()?;
        let cursor = store
            .open_cursor(None, Some(CursorDirection::Next))
            .await
            .err_into()?;

        let mut cursor = match cursor {
            Some(cursor) => cursor,
            None => {
                return Ok(Box::pin(empty()));
            }
        };

        let mut rows = Vec::new();
        let mut current_key = cursor.key().err_into()?;
        let mut current_row = cursor.value().err_into()?;

        while !current_key.is_null() {
            let key: JsonValue = current_key.into_serde().err_into()?;
            let key: Key = Value::try_from(key)?.try_into()?;

            let row = convert(current_row, column_defs.as_deref())?;

            rows.push((key, row));

            cursor.advance(1).await.err_into()?;
            current_key = cursor.key().err_into()?;
            current_row = cursor.value().err_into()?;
        }

        transaction.commit().await.err_into()?;

        let rows = rows.into_iter().map(Ok);
        Ok(Box::pin(iter(rows)))
    }
}

#[async_trait(?Send)]
impl StoreMut for IdbStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let version = self.database.version().err_into()? + 1;
        self.database.close();

        let error = Arc::new(Mutex::new(None));
        let open_request = {
            let error = Arc::clone(&error);
            let table_name = schema.table_name.to_owned();
            let mut open_request = self
                .factory
                .open(self.namespace.as_str(), Some(version))
                .err_into()?;

            open_request.on_upgrade_needed(move |event| {
                let database = match event.database().err_into() {
                    Ok(database) => database,
                    Err(e) => {
                        let mut error = match error.lock() {
                            Ok(error) => error,
                            Err(_) => {
                                let msg = JsValue::from_str("infallible - lock acquire failed");
                                console::error_1(&msg);
                                return;
                            }
                        };

                        *error = Some(e);
                        return;
                    }
                };

                let mut params = ObjectStoreParams::new();
                params.auto_increment(true);

                if let Err(e) = database.create_object_store(&table_name, params).err_into() {
                    let mut error = match error.lock() {
                        Ok(error) => error,
                        Err(_) => {
                            let msg = JsValue::from_str("infallible - lock acquire failed");
                            console::error_1(&msg);
                            return;
                        }
                    };

                    *error = Some(e);
                }
            });

            open_request
        };

        self.database = open_request.await.err_into()?;
        if let Some(e) = Arc::try_unwrap(error)
            .map_err(|_| Error::StorageMsg("infallible - Arc::try_unwrap failed".to_owned()))?
            .into_inner()
            .err_into()?
        {
            return Err(e);
        }

        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadWrite)
            .err_into()?;
        let store = transaction.object_store(SCHEMA_STORE).err_into()?;

        let key = JsValue::from_str(&schema.table_name);
        let schema = JsValue::from(schema.to_ddl());
        store.add(&schema, Some(&key)).await.err_into()?;

        transaction.commit().await.err_into()
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let version = self.database.version().err_into()? + 1;
        self.database.close();

        let error = Arc::new(Mutex::new(None));
        let open_request = {
            let error = Arc::clone(&error);
            let table_name = table_name.to_owned();
            let mut open_request = self
                .factory
                .open(self.namespace.as_str(), Some(version))
                .err_into()?;

            open_request.on_upgrade_needed(move |event| {
                let database = match event.database().err_into() {
                    Ok(database) => database,
                    Err(e) => {
                        let mut error = match error.lock() {
                            Ok(error) => error,
                            Err(_) => {
                                let msg = JsValue::from_str("infallible - lock acquire failed");
                                console::error_1(&msg);
                                return;
                            }
                        };

                        *error = Some(e);
                        return;
                    }
                };

                if !database
                    .store_names()
                    .iter()
                    .any(|name| name == &table_name)
                {
                    return;
                }

                if let Err(e) = database.delete_object_store(table_name.as_str()).err_into() {
                    let mut error = match error.lock() {
                        Ok(error) => error,
                        Err(_) => {
                            let msg = JsValue::from_str("infallible - lock acquire failed");
                            console::error_1(&msg);
                            return;
                        }
                    };

                    *error = Some(e);
                }
            });

            open_request
        };

        self.database = open_request.await.err_into()?;
        if let Some(e) = Arc::try_unwrap(error)
            .map_err(|_| Error::StorageMsg("infallible - Arc::try_unwrap failed".to_owned()))?
            .into_inner()
            .err_into()?
        {
            return Err(e);
        }

        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadWrite)
            .err_into()?;
        let store = transaction.object_store(SCHEMA_STORE).err_into()?;

        let key = JsValue::from_str(table_name);
        store.delete(Query::from(key)).await.err_into()?;

        transaction.commit().await.err_into()
    }

    async fn append_data(&mut self, table_name: &str, new_rows: Vec<DataRow>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadWrite)
            .err_into()?;
        let store = transaction.object_store(table_name).err_into()?;

        for data_row in new_rows {
            let row = match data_row {
                DataRow::Vec(values) => Value::List(values),
                DataRow::Map(values) => Value::Map(values),
            };

            let row = JsonValue::try_from(row)?;
            let row = JsValue::from_serde(&row).err_into()?;

            store.add(&row, None).await.err_into()?;
        }

        transaction.commit().await.err_into()
    }

    async fn insert_data(&mut self, table_name: &str, new_rows: Vec<(Key, DataRow)>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadWrite)
            .err_into()?;
        let store = transaction.object_store(table_name).err_into()?;

        for (key, data_row) in new_rows {
            let row = match data_row {
                DataRow::Vec(values) => Value::List(values),
                DataRow::Map(values) => Value::Map(values),
            };

            let row = JsonValue::try_from(row)?;
            let row = JsValue::from_serde(&row).err_into()?;

            let key: JsonValue = Value::from(key).try_into()?;
            let key = JsValue::from_serde(&key).err_into()?;

            store.put(&row, Some(&key)).await.err_into()?;
        }

        transaction.commit().await.err_into()
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadWrite)
            .err_into()?;
        let store = transaction.object_store(table_name).err_into()?;

        for key in keys {
            let key: JsonValue = Value::from(key).try_into()?;
            let key = JsValue::from_serde(&key).err_into()?;
            let key = Query::from(key);

            store.delete(key).await.err_into()?;
        }

        transaction.commit().await.err_into()
    }
}

impl gluesql_core::store::AlterTable for IdbStorage {}
impl gluesql_core::store::Index for IdbStorage {}
impl gluesql_core::store::IndexMut for IdbStorage {}
impl gluesql_core::store::Transaction for IdbStorage {}
impl Metadata for IdbStorage {}
impl gluesql_core::store::CustomFunction for IdbStorage {}
impl gluesql_core::store::CustomFunctionMut for IdbStorage {}
