use std::collections::HashMap;
use std::vec;

use bson::doc;
use bson::Bson;
use gluesql_core::prelude::Glue;
use gluesql_core::prelude::Payload;
use gluesql_core::prelude::Value;
use gluesql_mongo_storage::get_collection_options;
use gluesql_mongo_storage::MongoStorage;

#[tokio::test]
async fn mongo_types() {
    let conn_str = "mongodb://localhost:27017";

    let storage = MongoStorage::new(conn_str, "mongo_types")
        .await
        .expect("MongoStorage::new");
    storage.drop_database().await.expect("database dropped");

    let labels = vec![
        "col_javascript".to_owned(),
        "col_javascriptWithScope".to_owned(),
    ];
    let column_types = doc! {
        "col_javascript": { "bsonType": ["javascript"], "title": "TEXT" },
        "col_javascriptWithScope": { "bsonType": ["javascriptWithScope"], "title": "TEXT" }
    };

    let options = get_collection_options(labels, column_types);

    let table_name = "mongo_type_collection";

    storage
        .db
        .create_collection(table_name, options)
        .await
        .expect("create_collection");

    let data = doc! {
        "col_javascript": Bson::JavaScriptCode("function add(a, b) { return a + b; }".to_owned()),
        "col_javascriptWithScope": Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
            code: "function sub(a, b) { return a - b; }".to_owned(),
            scope: doc! { "a": 1, "b": 2 }
        })
    };

    storage
        .db
        .collection(table_name)
        .insert_one(data, None)
        .await
        .expect("insert_data");

    let mut glue = Glue::new(storage);

    let cases = vec![(
        glue.execute("SELECT * FROM mongo_type_collection").await,
        Ok(Payload::Select {
            labels: vec![
                "col_javascript".to_owned(),
                "col_javascriptWithScope".to_owned(),
            ],
            rows: vec![vec![
                Value::Str("function add(a, b) { return a + b; }".to_owned()),
                Value::Map(HashMap::from([
                    (
                        "code".to_owned(),
                        Value::Str("function sub(a, b) { return a - b; }".to_owned()),
                    ),
                    (
                        "scope".to_owned(),
                        Value::Map(HashMap::from([
                            ("a".to_owned(), Value::I32(1)),
                            ("b".to_owned(), Value::I32(2)),
                        ])),
                    ),
                ])),
            ]],
        }),
    )];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
