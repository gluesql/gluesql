use idb::{CursorDirection, KeyRange};
use wasm_bindgen::JsValue;

use crate::{
    key::{self, retrieve_key},
    storage_error::StorageError,
    IndexeddbStorage, DATA_STORE, SCHEMA_STORE,
};
use {
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Row, Schema},
        result::Result,
        store::{RowIter, Store},
    },
    std::str,
};

#[async_trait(?Send)]
impl Store for IndexeddbStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], idb::TransactionMode::ReadOnly)
            .map_err(StorageError::Idb)?;

        let store = transaction
            .object_store(SCHEMA_STORE)
            .map_err(StorageError::Idb)?;

        let entries = store
            .get_all(None, None)
            .await
            .map_err(StorageError::Idb)?
            .into_iter()
            .map(|v| serde_wasm_bindgen::from_value(v).map_err(StorageError::SerdeWasmBindgen))
            .collect::<std::result::Result<Vec<Schema>, _>>()?;

        transaction.done().await.map_err(StorageError::Idb)?;

        Ok(entries)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], idb::TransactionMode::ReadOnly)
            .map_err(StorageError::Idb)?;

        let store = transaction
            .object_store(SCHEMA_STORE)
            .map_err(StorageError::Idb)?;

        let entry = store
            .get(JsValue::from_str(table_name))
            .await
            .map(|e| serde_wasm_bindgen::from_value::<Schema>(e).ok())
            .map_err(StorageError::Idb)?;

        transaction.done().await.map_err(StorageError::Idb)?;

        Ok(entry)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Row>> {
        let transaction = self
            .database
            .transaction(&[DATA_STORE], idb::TransactionMode::ReadOnly)
            .map_err(StorageError::Idb)?;

        let store = transaction
            .object_store(DATA_STORE)
            .map_err(StorageError::Idb)?;

        let key = key::convert_key(table_name, key);

        let entry = store
            .get(JsValue::from_str(&key))
            .await
            .map(|e| serde_wasm_bindgen::from_value(e).ok())
            .map_err(StorageError::Idb)?;

        transaction.done().await.map_err(StorageError::Idb)?;

        Ok(entry)
    }
    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let transaction = self
            .database
            .transaction(&[DATA_STORE], idb::TransactionMode::ReadOnly)
            .map_err(StorageError::Idb)?;

        let store = transaction
            .object_store(DATA_STORE)
            .map_err(StorageError::Idb)?;

        let lower_bound = format!("{}/", table_name); // TODO inclusive
        let upper_bound = format!("{}0", table_name); // 0 comes after / in ascii

        let mut cursor = store
            .open_cursor(
                Some(idb::Query::KeyRange(
                    KeyRange::bound(
                        &JsValue::from_str(&lower_bound),
                        &JsValue::from_str(&upper_bound),
                        None,
                        None,
                    )
                    .map_err(StorageError::Idb)?,
                )),
                Some(CursorDirection::Next),
            )
            .await
            .map_err(StorageError::Idb)?;

        let mut entries: Vec<Result<(Key, Row)>> = vec![];
        while cursor.key().map_or(false, |v| !v.is_null()) {
            let key = cursor.key().map_err(StorageError::Idb)?;
            let key = key
                .as_string()
                .ok_or_else(|| StorageError::KeyParseError(format!("{:?}", key)))?;
            let key = retrieve_key(table_name, &key)?;

            let value = cursor.value().map_err(StorageError::Idb)?;
            let value =
                serde_wasm_bindgen::from_value(value).map_err(StorageError::SerdeWasmBindgen)?;

            entries.push(Ok((key, value)));

            cursor.next(None).await.map_err(StorageError::Idb)?;
        }

        transaction.done().await.map_err(StorageError::Idb)?;

        Ok(Box::new(entries.into_iter()))
    }
}
