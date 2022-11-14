use idb::KeyRange;
use wasm_bindgen::JsValue;

use crate::{storage_error::StorageError, IndexeddbStorage, DATA_STORE, SCHEMA_STORE};

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

        let schema =
            serde_wasm_bindgen::to_value(schema).map_err(StorageError::SerdeWasmBindgen)?;

        store.add(&schema, None).await.map_err(StorageError::Idb)?;

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

        let lower_bound = format!("{}/", table_name);
        let upper_bound = format!("{}0", table_name);

        data_store
            .delete(idb::Query::KeyRange(
                KeyRange::bound(
                    &JsValue::from_str(&lower_bound),
                    &JsValue::from_str(&upper_bound),
                    None,
                    None,
                )
                .map_err(StorageError::Idb)?,
            ))
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
            let key = format!("{}/{}", table_name, id); // TODO reusable function

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
            let key: Vec<_> = key.to_cmp_be_bytes().iter().map(u8::to_string).collect();
            let key = format!("{}/{}", table_name, key.join(",")); // TODO reusable function

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

    async fn delete_data(&self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let transaction = self
            .database
            .transaction(&[DATA_STORE], idb::TransactionMode::ReadWrite)
            .map_err(StorageError::Idb)?;

        let store = transaction
            .object_store(DATA_STORE)
            .map_err(StorageError::Idb)?;

        for key in keys {
            let key: Vec<_> = key.to_cmp_be_bytes().iter().map(u8::to_string).collect();
            let key = format!("{}/{}", table_name, key.join(",")); // TODO reusable function

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
