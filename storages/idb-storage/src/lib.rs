#![deny(clippy::str_to_string)]

mod convert;

use {
    async_trait::async_trait,
    convert::convert,
    gloo_utils::format::JsValueSerdeExt,
    gluesql_core::{
        data::{Key, Schema, Value},
        result::{Error, MutResult, Result, TrySelf},
        store::{DataRow, RowIter, Store, StoreMut},
    },
    idb::{CursorDirection, Database, Factory, ObjectStoreParams, Query, TransactionMode},
    serde::{Deserialize, Serialize},
    serde_json::Value as JsonValue,
    uuid::Uuid,
    wasm_bindgen::JsValue,
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
        let factory = Factory::new().map_err(|e| Error::StorageMsg(e.to_string()))?;

        // let namespace = Uuid::new_v4().to_string();

        let namespace = namespace
            .as_ref()
            .map(String::as_str)
            .unwrap_or(DEFAULT_NAMESPACE)
            .to_owned();
        // panic!("hey {namespace}");
        let mut open_request = factory.open(namespace.as_str(), None).unwrap();
        open_request.on_upgrade_needed(move |event| {
            let database = event.database().unwrap();

            database
                .create_object_store(SCHEMA_STORE, ObjectStoreParams::new())
                .unwrap();
        });

        let database = open_request.await.unwrap();
        // panic!("hey {namespace}");

        Ok(Self {
            namespace,
            factory,
            database,
        })
    }

    pub async fn delete(&self) -> Result<()> {
        self.factory
            .delete(&self.namespace)
            .await
            .map_err(|e| Error::StorageMsg(e.to_string()))
    }

    /*
    pub fn raw(&self) -> web_sys::Storage {
        match self.storage_type {
            IdbStorageType::Local => LocalStorage::raw(),
            IdbStorageType::Session => SessionStorage::raw(),
        }
    }

    pub fn get<T>(&self, key: impl AsRef<str>) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let value = match self.storage_type {
            IdbStorageType::Local => LocalStorage::get(key),
            IdbStorageType::Session => SessionStorage::get(key),
        };

        match value {
            Ok(value) => Ok(Some(value)),
            Err(StorageError::KeyNotFound(_)) => Ok(None),
            Err(e) => Err(Error::StorageMsg(e.to_string())),
        }
    }

    pub fn set<T>(&self, key: impl AsRef<str>, value: T) -> Result<()>
    where
        T: Serialize,
    {
        match self.storage_type {
            IdbStorageType::Local => LocalStorage::set(key, value),
            IdbStorageType::Session => SessionStorage::set(key, value),
        }
        .map_err(|e| Error::StorageMsg(e.to_string()))
    }

    pub fn delete(&self, key: impl AsRef<str>) {
        match self.storage_type {
            IdbStorageType::Local => LocalStorage::delete(key),
            IdbStorageType::Session => SessionStorage::delete(key),
        }
    }
    */
}

#[async_trait(?Send)]
impl Store for IdbStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadOnly)
            .unwrap();

        let store = transaction.object_store(SCHEMA_STORE).unwrap();
        let schemas = store.get_all(None, None).await.unwrap(); // Vec<JsValue>
        let schemas = schemas
            .into_iter()
            .map(|v| serde_wasm_bindgen::from_value(v).unwrap())
            .collect::<Vec<Schema>>();

        Ok(schemas)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadOnly)
            .unwrap();

        let store = transaction.object_store(SCHEMA_STORE).unwrap();
        let schema = store.get(JsValue::from_str(table_name)).await.unwrap(); // Vec<JsValue>
        let schema = schema.map(|v| serde_wasm_bindgen::from_value(v).unwrap());

        // panic!("fetch_schema: {table_name}, {}", schema.is_some());

        Ok(schema)
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadOnly)
            .unwrap();

        let store = transaction.object_store(table_name).unwrap();

        todo!();
        /*
        let path = format!("{}/{}", DATA_PATH, table_name);
        let row = self
            .get::<Vec<(Key, DataRow)>>(path)?
            .unwrap_or_default()
            .into_iter()
            .find_map(|(key, row)| (&key == target).then_some(row));

        Ok(row)
        */
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let Schema { column_defs, .. } = self.fetch_schema(table_name).await?.unwrap();
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadOnly)
            .unwrap();

        let store = transaction.object_store(table_name).unwrap();
        let cursor = store
            .open_cursor(None, Some(CursorDirection::Next))
            .await
            .unwrap();

        let mut cursor = match cursor {
            Some(cursor) => cursor,
            None => {
                return Ok(Box::new(Vec::new().into_iter()));
            }
        };

        let mut rows = Vec::new();
        let mut current_key = cursor.key().unwrap();
        let mut current_row = cursor.value().unwrap();

        while !current_key.is_null() {
            let key: JsonValue = current_key.into_serde().unwrap();
            let key: Key = Value::try_from(key)?.try_into()?;

            let row = convert(current_row, column_defs.as_ref().map(Vec::as_slice))?;

            rows.push((key, row));

            let result = cursor.advance(1).await;
            if result.is_err() {
                break;
            }

            current_key = cursor.key().unwrap();
            current_row = cursor.value().unwrap();
        }

        /*
        let key = cursor.key().unwrap();
        let key: JsonValue = key.into_serde().unwrap();
        let key: Key = Value::try_from(key)?.try_into()?;

        let row = cursor.value().unwrap();
        let row = convert(row, column_defs.as_ref().map(Vec::as_slice))?;
        let mut rows = vec![(key, row)].into_iter().map(Ok);

        rows.advance(1).unwrap();
        */

        let rows = rows.into_iter().map(Ok);

        Ok(Box::new(rows))

        /*
        let rows = store.get_all(None, None).await.unwrap(); // Vec<JsValue>
        let rows = rows
            .into_iter()
            .map(move |v| {
                let columns_defs = column_defs.as_ref().map(Vec::as_slice);

                convert(v, column_defs).map(|row| (Key::None, row))
            });
            // .map(|v| serde_wasm_bindgen::from_value(v).unwrap()) // todo!
            */

        /*
        let path = format!("{}/{}", DATA_PATH, table_name);
        let rows = self
            .get::<Vec<(Key, DataRow)>>(path)?
            .unwrap_or_default()
            .into_iter()
            .map(Ok);

        Ok(Box::new(rows))
        */
    }
}

impl IdbStorage {
    pub async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let table_name = schema.table_name.to_owned();
        let version = self.database.version().unwrap() + 1;

        self.database.close();

        // panic!("{} @ {version}", self.namespace);

        let mut open_request = self
            .factory
            .open(self.namespace.as_str(), Some(version))
            .unwrap();
        open_request.on_upgrade_needed(move |event| {
            // panic!("fail please 222222");
            let database = event.database().unwrap();
            // let table_name = &schema.table_name;

            let mut params = ObjectStoreParams::new();
            params.key_path(None);
            params.auto_increment(true);

            database.create_object_store(&table_name, params).unwrap();

            // how to deal with this

            //transaction.commit().await.unwrap();
            /*
            let schema = store.get(JsValue::from_str(table_name)).await.unwrap(); // Vec<JsValue>
            let schema = schema.map(|v| serde_wasm_bindgen::from_value(v).unwrap());
            */
        });

        self.database = open_request.await.unwrap();

        let table_name = &schema.table_name;
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadWrite)
            .unwrap();
        let store = transaction.object_store(SCHEMA_STORE).unwrap();

        // let key = serde_wasm_bindgen::to_value(&schema.table_name).unwrap();
        let key = JsValue::from_str(&schema.table_name);
        // let schema = JsValue::from_bool(true);
        let schema = JsValue::from_serde(&schema).unwrap();
        store.add(&schema, Some(&key)).await.unwrap();
        // store.add(&key, Some(&schema)).await.unwrap();

        transaction.commit().await.unwrap();
        /*

            let transaction = event.transaction().unwrap().unwrap();
            let store = transaction.object_store(table_name).unwrap();
        let schema = schema.clone();
        // let schema = JsonValue::try_from(row)?;

        // let key = Uuid::new_v4().to_string();


        transaction.commit().await.unwrap();
        */

        Ok(())
        /*
        let mut table_names: Vec<String> = self.get(TABLE_NAMES_PATH)?.unwrap_or_default();
        table_names.push(schema.table_name.clone());

        self.set(TABLE_NAMES_PATH, table_names)?;
        self.set(format!("{}/{}", SCHEMA_PATH, schema.table_name), schema)
        */
    }

    pub async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let version = self.database.version().unwrap() + 1;
        self.database.close();

        let mut open_request = self
            .factory
            .open(self.namespace.as_str(), Some(version))
            .unwrap();

        // let table_name = table_name.to_owned();
        let n = table_name.to_owned();
        open_request.on_upgrade_needed(move |event| {
            let table_name = n;
            let database = event.database().unwrap();

            if database
                .store_names()
                .iter()
                .any(|name| name == &table_name)
            {
                database.delete_object_store(table_name.as_str()).unwrap();
            }
        });

        self.database = open_request.await.unwrap();

        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], TransactionMode::ReadWrite)
            .unwrap();
        let store = transaction.object_store(SCHEMA_STORE).unwrap();

        // let key = serde_wasm_bindgen::to_value(&schema.table_name).unwrap();
        let key = JsValue::from_str(table_name);
        // let schema = JsValue::from_bool(true);
        store.delete(Query::from(key)).await.unwrap();

        transaction.commit().await.unwrap();

        Ok(())
        /*
        let mut table_names: Vec<String> = self.get(TABLE_NAMES_PATH)?.unwrap_or_default();
        table_names
            .iter()
            .position(|name| name == table_name)
            .map(|i| table_names.remove(i));

        self.set(TABLE_NAMES_PATH, table_names)?;
        self.delete(format!("{}/{}", SCHEMA_PATH, table_name));
        self.delete(format!("{}/{}", DATA_PATH, table_name));

        Ok(())
        */
    }

    pub async fn append_data(&mut self, table_name: &str, new_rows: Vec<DataRow>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadWrite)
            .unwrap();
        let store = transaction.object_store(table_name).unwrap();

        for data_row in new_rows.into_iter() {
            let row = match data_row {
                DataRow::Vec(values) => Value::List(values),
                DataRow::Map(values) => Value::Map(values),
            };

            let row = JsonValue::try_from(row)?;
            let row = JsValue::from_serde(&row).unwrap();

            // panic!("{row:?}");

            // let key = Uuid::new_v4().to_string();
            // let key = JsValue::from_str(&key);
            // let key = serde_wasm_bindgen::to_value(&key).unwrap();

            store.add(&row, None).await.unwrap();
            // panic!("something inserted, {key:?} -> {row:?}");
        }

        transaction.commit().await.unwrap();
        Ok(())
        /*
        let path = format!("{}/{}", DATA_PATH, table_name);
        let rows = self.get::<Vec<(Key, DataRow)>>(&path)?.unwrap_or_default();
        let new_rows = new_rows.into_iter().map(|row| {
            let key = Key::Uuid(Uuid::new_v4().as_u128());

            (key, row)
        });

        let rows = rows.into_iter().chain(new_rows).collect::<Vec<_>>();

        self.set(path, rows)
        */
    }

    pub async fn insert_data(
        &mut self,
        table_name: &str,
        new_rows: Vec<(Key, DataRow)>,
    ) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadWrite)
            .unwrap();
        let store = transaction.object_store(table_name).unwrap();

        for (key, data_row) in new_rows.into_iter() {
            let row = match data_row {
                DataRow::Vec(values) => Value::List(values),
                DataRow::Map(values) => Value::Map(values),
            };

            let row = JsonValue::try_from(row)?;
            let row = JsValue::from_serde(&row).unwrap();

            // panic!("{row:?}");

            // let key = Uuid::new_v4().to_string();
            // let key = JsValue::from_str(&key);
            // let key = serde_wasm_bindgen::to_value(&key).unwrap();

            let key = match key {
                Key::I64(v) => v as f64,
                _ => todo!(),
            };

            let key = JsValue::from_f64(key);

            store.put(&row, Some(&key)).await.unwrap();
            // panic!("something inserted, {key:?} -> {row:?}");
        }

        transaction.commit().await.unwrap();
        Ok(())
        /*
        let path = format!("{}/{}", DATA_PATH, table_name);
        let mut rows = self.get::<Vec<(Key, DataRow)>>(&path)?.unwrap_or_default();

        for (key, row) in new_rows.into_iter() {
            if let Some(i) = rows.iter().position(|(k, _)| k == &key) {
                rows[i] = (key, row);
            } else {
                rows.push((key, row));
            }
        }

        self.set(path, rows)
        */
    }

    pub async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[table_name], TransactionMode::ReadWrite)
            .unwrap();
        let store = transaction.object_store(table_name).unwrap();

        for key in keys.into_iter() {
            // panic!("{row:?}");

            // let key = Uuid::new_v4().to_string();
            // let key = JsValue::from_str(&key);
            // let key = serde_wasm_bindgen::to_value(&key).unwrap();

            let key = match key {
                Key::I64(v) => v as f64,
                _ => todo!(),
            };

            let key = JsValue::from_f64(key);
            let key = Query::from(key);

            store.delete(key).await.unwrap();
            // panic!("something inserted, {key:?} -> {row:?}");
        }

        transaction.commit().await.unwrap();
        Ok(())

        /*
        let path = format!("{}/{}", DATA_PATH, table_name);
        let mut rows = self.get::<Vec<(Key, DataRow)>>(&path)?.unwrap_or_default();

        for key in keys.iter() {
            if let Some(i) = rows.iter().position(|(k, _)| k == key) {
                rows.remove(i);
            }
        }

        self.set(path, rows)
        */
    }
}

#[async_trait(?Send)]
impl StoreMut for IdbStorage {
    async fn insert_schema(mut self, schema: &Schema) -> MutResult<Self, ()> {
        IdbStorage::insert_schema(&mut self, schema)
            .await
            .try_self(self)
    }

    async fn delete_schema(mut self, table_name: &str) -> MutResult<Self, ()> {
        IdbStorage::delete_schema(&mut self, table_name)
            .await
            .try_self(self)
    }

    async fn append_data(mut self, table_name: &str, rows: Vec<DataRow>) -> MutResult<Self, ()> {
        IdbStorage::append_data(&mut self, table_name, rows)
            .await
            .try_self(self)
    }

    async fn insert_data(
        mut self,
        table_name: &str,
        rows: Vec<(Key, DataRow)>,
    ) -> MutResult<Self, ()> {
        IdbStorage::insert_data(&mut self, table_name, rows)
            .await
            .try_self(self)
    }

    async fn delete_data(mut self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        IdbStorage::delete_data(&mut self, table_name, keys)
            .await
            .try_self(self)
    }
}

#[cfg(feature = "alter-table")]
impl gluesql_core::store::AlterTable for IdbStorage {}
#[cfg(feature = "index")]
impl gluesql_core::store::Index for IdbStorage {}
#[cfg(feature = "index")]
impl gluesql_core::store::IndexMut for IdbStorage {}
#[cfg(feature = "transaction")]
impl gluesql_core::store::Transaction for IdbStorage {}
