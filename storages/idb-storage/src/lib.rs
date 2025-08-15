#![cfg(target_arch = "wasm32")]
#![deny(clippy::str_to_string)]

pub mod convert;
mod error;

use {
    async_trait::async_trait,
    convert::convert,
    error::{ErrInto, StoreReqIntoFuture},
    futures::stream::{empty, iter},
    gloo_utils::format::JsValueSerdeExt,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::{Error, Result},
        store::{DataRow, Metadata, RowIter, Store, StoreMut},
    },
    idb::{
        CursorDirection, Database, DatabaseEvent, Factory, ObjectStoreParams, Query,
        TransactionMode,
    },
    send_wrapper::SendWrapper,
    serde_json::Value as JsonValue,
    std::sync::{Arc, Mutex},
    wasm_bindgen::JsValue,
    web_sys::console,
};

trait SendWrapperExt: Sized {
    fn send_wrapper(self) -> SendWrapper<Self> {
        SendWrapper::new(self)
    }
}

impl<T> SendWrapperExt for T {}

const SCHEMA_STORE: &str = "gluesql-schema";
const DEFAULT_NAMESPACE: &str = "gluesql";

enum AlterType {
    InsertSchema,
    DeleteSchema,
}

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
        let mut error = error
            .lock()
            .map_err(|_| Error::StorageMsg("infallible - lock acquire failed".to_owned()))?;
        if let Some(e) = error.take() {
            return Err(e);
        }

        Ok(Self {
            namespace,
            factory,
            database,
        })
    }

    pub async fn delete(&self) -> Result<()> {
        self.factory
            .delete(&self.namespace)
            .into_future()
            .send_wrapper()
            .await
            .err_into()
    }

    async fn alter_object_store(
        &mut self,
        table_name: String,
        alter_type: AlterType,
    ) -> Result<()> {
        let version = self.database.version().err_into()? + 1;
        self.database.close();

        let error = Arc::new(Mutex::new(None));
        let open_request = {
            let error = Arc::clone(&error);
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

                let err = match alter_type {
                    AlterType::InsertSchema => {
                        let mut params = ObjectStoreParams::new();
                        params.auto_increment(true);

                        database
                            .create_object_store(&table_name, params)
                            .err_into()
                            .map(|_| ())
                    }
                    AlterType::DeleteSchema => {
                        if !database
                            .store_names()
                            .iter()
                            .any(|name| name == &table_name)
                        {
                            return;
                        }
                        database
                            .delete_object_store(table_name.as_str())
                            .err_into()
                            .map(|_| ())
                    }
                };

                if let Err(e) = err {
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

        self.database = open_request.into_future().send_wrapper().await.err_into()?;
        let mut error = error
            .lock()
            .map_err(|_| Error::StorageMsg("infallible - lock acquire failed".to_owned()))?;
        if let Some(e) = error.take() {
            return Err(e);
        }

        Ok(())
    }
}

#[async_trait]
impl Store for IdbStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadOnly)
            .err_into()?
            .send_wrapper();

        let store = transaction
            .object_store(SCHEMA_STORE)
            .err_into()?
            .send_wrapper();
        let schemas = store
            .get_all(None, None)
            .into_future()
            .send_wrapper()
            .await
            .err_into()?;
        let schemas = schemas
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
            .collect::<Result<Vec<Schema>>>()?;

        transaction
            .take()
            .commit()
            .into_future()
            .send_wrapper()
            .await
            .err_into()?;
        Ok(schemas)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadOnly)
            .err_into()?
            .send_wrapper();

        let store = transaction
            .object_store(SCHEMA_STORE)
            .err_into()?
            .send_wrapper();
        let schema = store
            .get(JsValue::from_str(table_name))
            .into_future()
            .send_wrapper()
            .await
            .err_into()?
            .and_then(|schema| JsValue::as_string(&schema))
            .map(|schema| Schema::from_ddl(schema.as_str()))
            .transpose()?;

        transaction
            .take()
            .commit()
            .into_future()
            .send_wrapper()
            .await
            .err_into()?;
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
            .err_into()?
            .send_wrapper();

        let store = transaction
            .object_store(table_name)
            .err_into()?
            .send_wrapper();

        let key: Value = target.clone().into();
        let key: JsonValue = key.try_into()?;
        let key = JsValue::from_serde(&key).err_into()?;
        let row = store
            .get(key)
            .into_future()
            .send_wrapper()
            .await
            .err_into()?;
        let row = row
            .map(|row| convert(row, column_defs.as_deref()))
            .transpose()?;

        transaction
            .take()
            .commit()
            .into_future()
            .send_wrapper()
            .await
            .err_into()?;

        Ok(row)
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        let column_defs = self
            .fetch_schema(table_name)
            .await?
            .and_then(|schema| schema.column_defs);
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadOnly)
            .err_into()?
            .send_wrapper();

        let store = transaction
            .object_store(table_name)
            .err_into()?
            .send_wrapper();
        let mut cursor = match store
            .open_cursor(None, Some(CursorDirection::Next))
            .into_future()
            .send_wrapper()
            .await
            .err_into()?
        {
            Some(cursor) => cursor.into_managed().send_wrapper(),
            None => {
                return Ok(Box::pin(empty()));
            }
        };

        let mut rows = Vec::new();
        let mut current_key = cursor
            .key()
            .err_into()?
            .unwrap_or(JsValue::NULL)
            .send_wrapper();
        let mut current_row = cursor
            .value()
            .err_into()?
            .unwrap_or(JsValue::NULL)
            .send_wrapper();

        while !current_key.as_ref().is_null() {
            {
                let key_js = current_key.take();
                let row_js = current_row.take();

                let key: JsonValue = key_js.into_serde().err_into()?;
                let key: Key = Value::try_from(key)?.try_into()?;

                let row = convert(row_js, column_defs.as_deref())?;

                rows.push((key, row));
            }

            cursor.advance(1).send_wrapper().await.err_into()?;
            current_key = cursor
                .key()
                .err_into()?
                .unwrap_or(JsValue::NULL)
                .send_wrapper();
            current_row = cursor
                .value()
                .err_into()?
                .unwrap_or(JsValue::NULL)
                .send_wrapper();
        }

        transaction
            .take()
            .commit()
            .into_future()
            .send_wrapper()
            .await
            .err_into()?;

        let rows = rows.into_iter().map(Ok);
        Ok(Box::pin(iter(rows)))
    }
}

#[async_trait]
impl StoreMut for IdbStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let schema_exists = self
            .fetch_schema(&schema.table_name)
            .await
            .map_err(|e| e.to_string())
            .map_err(Error::StorageMsg)?
            .is_some();

        if !schema_exists {
            self.alter_object_store(schema.table_name.to_owned(), AlterType::InsertSchema)
                .await?;
        }

        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadWrite)
            .err_into()?
            .send_wrapper();
        let store = transaction
            .object_store(SCHEMA_STORE)
            .err_into()?
            .send_wrapper();

        let key = JsValue::from_str(&schema.table_name).send_wrapper();
        let schema = JsValue::from(schema.to_ddl()).send_wrapper();

        if schema_exists {
            store
                .put(&schema, Some(&key))
                .into_future()
                .send_wrapper()
                .await
                .err_into()?;
        } else {
            store
                .add(&schema, Some(&key))
                .into_future()
                .send_wrapper()
                .await
                .err_into()?;
        };

        transaction
            .take()
            .commit()
            .into_future()
            .send_wrapper()
            .await
            .err_into()
            .map(|_| ())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.alter_object_store(table_name.to_owned(), AlterType::DeleteSchema)
            .await?;

        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadWrite)
            .err_into()?
            .send_wrapper();
        let store = transaction
            .object_store(SCHEMA_STORE)
            .err_into()?
            .send_wrapper();

        let key = JsValue::from_str(table_name);
        store
            .delete(Query::from(key))
            .into_future()
            .send_wrapper()
            .await
            .err_into()?;

        transaction
            .take()
            .commit()
            .into_future()
            .send_wrapper()
            .await
            .err_into()
            .map(|_| ())
    }

    async fn append_data(&mut self, table_name: &str, new_rows: Vec<DataRow>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadWrite)
            .err_into()?
            .send_wrapper();
        let store = transaction
            .object_store(table_name)
            .err_into()?
            .send_wrapper();

        for data_row in new_rows {
            let row = match data_row {
                DataRow::Vec(values) => Value::List(values),
                DataRow::Map(values) => Value::Map(values),
            };

            let row = JsonValue::try_from(row)?;
            let row = JsValue::from_serde(&row).err_into()?;
            let row = row.send_wrapper();

            store
                .add(&row, None)
                .into_future()
                .send_wrapper()
                .await
                .err_into()?;
        }

        transaction
            .take()
            .commit()
            .into_future()
            .send_wrapper()
            .await
            .err_into()
            .map(|_| ())
    }

    async fn insert_data(&mut self, table_name: &str, new_rows: Vec<(Key, DataRow)>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadWrite)
            .err_into()?
            .send_wrapper();
        let store = transaction
            .object_store(table_name)
            .err_into()?
            .send_wrapper();

        for (key, data_row) in new_rows {
            let row = match data_row {
                DataRow::Vec(values) => Value::List(values),
                DataRow::Map(values) => Value::Map(values),
            };

            let row = JsonValue::try_from(row)?;
            let row = JsValue::from_serde(&row).err_into()?;
            let row = row.send_wrapper();

            let key: JsonValue = Value::from(key).try_into()?;
            let key = JsValue::from_serde(&key).err_into()?;
            let key = key.send_wrapper();

            store
                .put(&row, Some(&key))
                .into_future()
                .send_wrapper()
                .await
                .err_into()?;
        }

        transaction
            .take()
            .commit()
            .into_future()
            .send_wrapper()
            .await
            .err_into()
            .map(|_| ())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadWrite)
            .err_into()?
            .send_wrapper();
        let store = transaction
            .object_store(table_name)
            .err_into()?
            .send_wrapper();

        for key in keys {
            let key: JsonValue = Value::from(key).try_into()?;
            let key = JsValue::from_serde(&key).err_into()?;
            let key = Query::from(key);

            store
                .delete(key)
                .into_future()
                .send_wrapper()
                .await
                .err_into()?;
        }

        transaction
            .take()
            .commit()
            .into_future()
            .send_wrapper()
            .await
            .err_into()
            .map(|_| ())
    }
}

impl gluesql_core::store::AlterTable for IdbStorage {}
impl gluesql_core::store::Index for IdbStorage {}
impl gluesql_core::store::IndexMut for IdbStorage {}
impl gluesql_core::store::Transaction for IdbStorage {}
impl Metadata for IdbStorage {}
impl gluesql_core::store::CustomFunction for IdbStorage {}
impl gluesql_core::store::CustomFunctionMut for IdbStorage {}

/// `IdbStorage` holds `web_sys` types that are not `Send` by default, but it
/// is used only on `wasm32` targets where execution happens on a single
/// thread. No concurrent access can occur, so it is safe to move
/// `IdbStorage` between threads.
unsafe impl Send for IdbStorage {}

/// WebAssembly runs `IdbStorage` on a single thread, preventing concurrent
/// access. This makes it safe to share references to `IdbStorage` across
/// threads even though the underlying types are not `Sync`.
unsafe impl Sync for IdbStorage {}
