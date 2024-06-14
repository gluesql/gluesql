#![cfg(feature = "test-mongo")]

use {
    bson::{doc, Document},
    gluesql_core::prelude::{Glue, Payload},
    gluesql_mongo_storage::{utils::Validator, MongoStorage},
    mongodb::{options::IndexOptions, IndexModel},
    std::vec,
};

#[tokio::test]
async fn mongo_indexes() {
    let conn_str = "mongodb://localhost:27017";

    let storage = MongoStorage::new(conn_str, "mongo_indexes")
        .await
        .expect("MongoStorage::new");
    storage.drop_database().await.expect("database dropped");

    let labels = vec!["id".to_owned(), "name".to_owned()];
    let column_types = doc! {
        "id": { "bsonType": ["int"], "title": "INT" },
        "name": { "bsonType": ["string"], "title": "TEXT" },
    };

    let options = Validator::new(labels, column_types, Vec::new(), None)
        .unwrap()
        .to_options();

    let table_name = "collection_with_composite_index";

    storage
        .db
        .create_collection(table_name, options)
        .await
        .expect("create_collection");

    let index_options = IndexOptions::builder()
        .name("ignored_composite_index".to_owned())
        .build();
    let index_model = IndexModel::builder()
        .keys(doc! {"id": 1, "name":1 })
        .options(index_options)
        .build();
    let collection = storage.db.collection::<Document>(table_name);
    collection.create_index(index_model, None).await.unwrap();

    let mut glue = Glue::new(storage);

    let cases = vec![(
        glue.execute(format! {"SELECT * FROM {table_name}"}).await,
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "name".to_owned()],
            rows: vec![],
        }),
    )];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
