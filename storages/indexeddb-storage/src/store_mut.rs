use wasm_bindgen::JsValue;
use wasm_bindgen_test::console_log;

use crate::{
    key::{convert_key, generate_key},
    query::table_data_query,
    storage_error::StorageError,
    IndexeddbStorage, DATA_STORE, SCHEMA_STORE,
};

use {
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Row, Schema},
        result::{MutResult, Result},
        store::StoreMut,
    },
};

impl IndexeddbStorage {
    async fn insert_schema(&self, schema: &Schema) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], idb::TransactionMode::ReadWrite)
            .map_err(StorageError::Idb)?;

        let store = transaction
            .object_store(SCHEMA_STORE)
            .map_err(StorageError::Idb)?;

        // if store
        //     .get(JsValue::from_str(&schema.table_name))
        //     .await
        //     .map(|e| serde_wasm_bindgen::from_value::<Schema>(e).ok())
        //     .map_err(StorageError::Idb)?
        //     .is_some(){
        //         return Err(gluesql_core::result::Error::Alter(()))
        //     }

        let schema =
            serde_wasm_bindgen::to_value(schema).map_err(StorageError::SerdeWasmBindgen)?;

        store.put(&schema, None).await.map_err(StorageError::Idb)?;

        transaction.commit().await.map_err(StorageError::Idb)?;

        Ok(())
    }

    async fn delete_schema(&self, table_name: &str) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE, DATA_STORE], idb::TransactionMode::ReadWrite)
            .map_err(StorageError::Idb)?;

        let schema_store = transaction
            .object_store(SCHEMA_STORE)
            .map_err(StorageError::Idb)?;
        schema_store
            .delete(JsValue::from_str(table_name))
            .await
            .map_err(StorageError::Idb)?;

        let data_store = transaction
            .object_store(DATA_STORE)
            .map_err(StorageError::Idb)?;

        data_store
            .delete(table_data_query(table_name)?)
            .await
            .map_err(StorageError::Idb)?;

        transaction.commit().await.map_err(StorageError::Idb)?;

        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<Row>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[DATA_STORE], idb::TransactionMode::ReadWrite)
            .map_err(StorageError::Idb)?;

        let store = transaction
            .object_store(DATA_STORE)
            .map_err(StorageError::Idb)?;

        for row in rows {
            let id = self.id_ctr;
            self.id_ctr += 1;
            let key = generate_key(table_name, id);

            console_log!("Data: {:?}", row);

            store
                .add(
                    &serde_wasm_bindgen::to_value(&row).map_err(StorageError::SerdeWasmBindgen)?,
                    Some(&JsValue::from_str(&key)),
                )
                .await
                .map_err(StorageError::Idb)?;
        }

        transaction.commit().await.map_err(StorageError::Idb)?;

        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Row)>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[DATA_STORE], idb::TransactionMode::ReadWrite)
            .map_err(StorageError::Idb)?;

        let store = transaction
            .object_store(DATA_STORE)
            .map_err(StorageError::Idb)?;

        for (key, row) in rows {
            self.id_ctr += 1;
            let key = convert_key(table_name, &key);

            store
                .put(
                    &serde_wasm_bindgen::to_value(&row).map_err(StorageError::SerdeWasmBindgen)?,
                    Some(&JsValue::from_str(&key)),
                )
                .await
                .map_err(StorageError::Idb)?;
        }

        transaction.commit().await.map_err(StorageError::Idb)?;

        Ok(())
    }

    async fn delete_data(&self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[DATA_STORE], idb::TransactionMode::ReadWrite)
            .map_err(StorageError::Idb)?;

        let store = transaction
            .object_store(DATA_STORE)
            .map_err(StorageError::Idb)?;

        for key in keys {
            let key = convert_key(table_name, &key);

            store
                .delete(JsValue::from_str(&key))
                .await
                .map_err(StorageError::Idb)?;
        }

        transaction.commit().await.map_err(StorageError::Idb)?;

        Ok(())
    }
}

#[async_trait(?Send)]
impl StoreMut for IndexeddbStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        match Self::insert_schema(&self, schema).await {
            Ok(()) => Ok((self, ())),
            Err(err) => Err((self, err)),
        }
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        match Self::delete_schema(&self, table_name).await {
            Ok(()) => Ok((self, ())),
            Err(err) => Err((self, err)),
        }
    }

    async fn append_data(mut self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        match Self::append_data(&mut self, table_name, rows).await {
            Ok(()) => Ok((self, ())),
            Err(err) => Err((self, err)),
        }
    }

    async fn insert_data(mut self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        match Self::insert_data(&mut self, table_name, rows).await {
            Ok(()) => Ok((self, ())),
            Err(err) => Err((self, err)),
        }
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        match Self::delete_data(&self, table_name, keys).await {
            Ok(()) => Ok((self, ())),
            Err(err) => Err((self, err)),
        }
    }
}
