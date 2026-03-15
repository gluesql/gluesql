#![cfg(feature = "test-mongo")]

use {
    gluesql_core::{
        data::Key,
        error::Error,
        prelude::{Glue, Value},
        store::{Store, StoreMut},
    },
    gluesql_mongo_storage::{MongoStorage, error::MongoStorageError},
};

#[tokio::test]
async fn mongo_storage_conflict_errors() {
    let conn_str = "mongodb://localhost:27017";

    let storage = MongoStorage::new(conn_str, "mongo_storage_conflict")
        .await
        .expect("MongoStorage::new");
    storage.drop_database().await.expect("database dropped");

    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Logs").await.unwrap();

    let actual = glue.storage.fetch_data("Logs", &Key::I64(1)).await;
    let expected = Err(Error::StorageMsg(
        MongoStorageError::ConflictFetchData.to_string(),
    ));
    assert_eq!(
        actual, expected,
        "fetch_data on schemaless table should return conflict error"
    );

    let actual = glue
        .storage
        .append_data("Logs", vec![vec![Value::I64(1)]])
        .await;
    let expected = Err(Error::StorageMsg(
        MongoStorageError::ConflictAppendData.to_string(),
    ));
    assert_eq!(
        actual, expected,
        "append_data with Vec<Value> should return conflict error"
    );

    let actual = glue
        .storage
        .insert_data("Logs", vec![(Key::Bytea(vec![0; 12]), vec![Value::I64(1)])])
        .await;
    let expected = Err(Error::StorageMsg(
        MongoStorageError::ConflictInsertData.to_string(),
    ));
    assert_eq!(
        actual, expected,
        "insert_data with non-map schemaless payload should return conflict error"
    );
}
