use idb::KeyRange;
use wasm_bindgen::JsValue;

use crate::{IndexeddbStorage, DATA_STORE, SCHEMA_STORE};

use {
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Row, Schema},
        result::MutResult,
        store::StoreMut,
    },
};

#[async_trait(?Send)]
impl StoreMut for IndexeddbStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE], idb::TransactionMode::ReadWrite)
            .unwrap();

        let store = transaction.object_store(SCHEMA_STORE).unwrap();

        let schema = serde_wasm_bindgen::to_value(schema).unwrap();

        store.add(&schema, None).await.unwrap();

        transaction.commit().await.unwrap();

        Ok((self, ()))
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let transaction = self
            .database
            .transaction(&[SCHEMA_STORE, DATA_STORE], idb::TransactionMode::ReadWrite)
            .unwrap();

        let schema_store = transaction.object_store(SCHEMA_STORE).unwrap();
        schema_store
            .delete(JsValue::from_str(table_name))
            .await
            .unwrap();

        let data_store = transaction.object_store(DATA_STORE).unwrap();

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
                .unwrap(),
            ))
            .await
            .unwrap();

        // TODO delete data

        transaction.commit().await.unwrap();

        Ok((self, ()))
    }

    async fn append_data(mut self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        let transaction = self
            .database
            .transaction(&[DATA_STORE], idb::TransactionMode::ReadWrite)
            .unwrap();

        let store = transaction.object_store(DATA_STORE).unwrap();

        for row in rows {
            let id = self.id_ctr;
            self.id_ctr += 1;
            let key = format!("{}/{}", table_name, id); // TODO reusable function

            store
                .add(
                    &serde_wasm_bindgen::to_value(&row).unwrap(),
                    Some(&JsValue::from_str(&key)),
                )
                .await
                .unwrap();
        }

        transaction.commit().await.unwrap();

        Ok((self, ()))
    }

    async fn insert_data(mut self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        let transaction = self
            .database
            .transaction(&[DATA_STORE], idb::TransactionMode::ReadWrite)
            .unwrap();

        let store = transaction.object_store(DATA_STORE).unwrap();

        for (key, row) in rows {
            self.id_ctr += 1;
            let key: Vec<_> = key.to_cmp_be_bytes().iter().map(u8::to_string).collect();
            let key = format!("{}/{}", table_name, key.join(",")); // TODO reusable function

            store
                .add(
                    &serde_wasm_bindgen::to_value(&row).unwrap(),
                    Some(&JsValue::from_str(&key)),
                )
                .await
                .unwrap();
        }

        transaction.commit().await.unwrap();

        Ok((self, ()))
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        let transaction = self
            .database
            .transaction(&[DATA_STORE], idb::TransactionMode::ReadWrite)
            .unwrap();

        let store = transaction.object_store(DATA_STORE).unwrap();

        for key in keys {
            let key: Vec<_> = key.to_cmp_be_bytes().iter().map(u8::to_string).collect();
            let key = format!("{}/{}", table_name, key.join(",")); // TODO reusable function

            store.delete(JsValue::from_str(&key)).await.unwrap();
        }

        transaction.commit().await.unwrap();

        Ok((self, ()))
    }
}
