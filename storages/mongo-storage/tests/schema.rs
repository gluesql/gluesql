#![cfg(feature = "test-mongo")]

use {
    bson::Document,
    gluesql_core::{prelude::Glue, store::Store},
    gluesql_mongo_storage::{MongoStorage, utils::Validator},
};

const CONNECTION_STRING: &str = "mongodb://localhost:27017";

fn storage(database: &str) -> MongoStorage {
    let storage = MongoStorage::new(CONNECTION_STRING, database).expect("MongoStorage::new");
    storage.drop_database().expect("database dropped");
    storage
}

#[test]
fn engine_round_trip() {
    let mut glue = Glue::new(storage("mongo_schema_engine_round_trip"));

    glue.execute("CREATE TABLE EngineTable (id INTEGER) ENGINE = MONGO;")
        .unwrap();

    let schema = glue.storage.fetch_schema("EngineTable").unwrap().unwrap();
    assert_eq!(schema.engine.as_deref(), Some("MONGO"));

    glue.storage.drop_database().expect("database dropped");
}

#[test]
fn legacy_schema_without_engine_remains_readable() {
    let storage = storage("mongo_legacy_schema_without_engine");
    let validator = Validator::new(Vec::new(), Document::new(), Vec::new(), None).unwrap();
    let description = validator
        .document
        .get_document("$jsonSchema")
        .unwrap()
        .get_str("description")
        .unwrap();
    assert_eq!(description, r#"{"foreign_keys":[],"comment":null}"#);

    storage
        .db
        .create_collection("LegacyTable", validator.to_options())
        .unwrap();

    let schema = storage.fetch_schema("LegacyTable").unwrap().unwrap();
    assert_eq!(schema.engine, None);

    storage.drop_database().expect("database dropped");
}
