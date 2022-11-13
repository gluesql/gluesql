use idb::{CursorDirection, KeyRange};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::console_log;

use crate::{IndexeddbStorage, DATA_STORE, SCHEMA_STORE};
use {
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Row, Schema},
        result::{Error, Result},
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
            .unwrap();

        let store = transaction.object_store(SCHEMA_STORE).unwrap();

        let entries = store
            .get_all(None, None)
            .await
            .unwrap()
            .into_iter()
            .map(|v| serde_wasm_bindgen::from_value(v).unwrap())
            .collect::<Vec<Schema>>();

        transaction.done().await.unwrap();

        Ok(entries)
    }
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], idb::TransactionMode::ReadOnly)
            .unwrap();

        let store = transaction.object_store(SCHEMA_STORE).unwrap();

        let entry = store
            .get(JsValue::from_str(table_name))
            .await
            .map(|e| serde_wasm_bindgen::from_value::<Schema>(e).ok())
            .unwrap();

        console_log!(
            "Getting schema {:?}: {:?}",
            JsValue::from_str(table_name),
            entry
        );

        transaction.done().await.unwrap();

        Ok(entry)
    }
    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Row>> {
        let transaction = self
            .database
            .transaction(&[DATA_STORE], idb::TransactionMode::ReadOnly)
            .unwrap();

        let store = transaction.object_store(DATA_STORE).unwrap();

        let key: Vec<_> = key.to_cmp_be_bytes().iter().map(u8::to_string).collect();
        let key = format!("{}/{}", table_name, key.join(",")); // TODO reusable function

        let entry = store
            .get(JsValue::from_str(&key))
            .await
            .map(|e| serde_wasm_bindgen::from_value(e).ok())
            .unwrap();

        transaction.done().await.unwrap();

        Ok(entry)
    }
    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let transaction = self
            .database
            .transaction(&[DATA_STORE], idb::TransactionMode::ReadOnly)
            .unwrap();

        let store = transaction.object_store(DATA_STORE).unwrap();

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
                    .unwrap(),
                )),
                Some(CursorDirection::Next),
            )
            .await
            .unwrap();

        let mut entries: Vec<Result<(Key, Row)>> = vec![];
        while cursor.key().map_or(false, |v| !v.is_null()) {
            // TODO proper function
            let key: Vec<u8> = cursor
                .key()
                .unwrap()
                .as_string()
                .unwrap()
                .chars()
                .skip_while(|c| *c != '/')
                .skip(1)
                .collect::<String>()
                .split(',')
                .map(|s| s.parse::<u8>().unwrap())
                .collect();
            let key = Key::Bytea(key);
            entries.push(Ok((
                key,
                serde_wasm_bindgen::from_value(cursor.value().unwrap()).unwrap(),
            )));

            cursor.next(None).await.unwrap();
        }

        transaction.done().await.unwrap();

        Ok(Box::new(entries.into_iter()))
    }
}
