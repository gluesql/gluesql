#![cfg(feature = "test-mongo")]

use {
    bson::{doc, Bson},
    gluesql_core::prelude::{Glue, Payload, Value},
    gluesql_mongo_storage::{utils::Validator, MongoStorage},
    std::{collections::HashMap, vec},
};

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
        "col_regex".to_owned(),
        "col_minKey".to_owned(),
        "col_maxKey".to_owned(),
    ];
    let column_types = doc! {
        "col_javascript": { "bsonType": ["javascript"], "title": "TEXT" },
        "col_javascriptWithScope": { "bsonType": ["javascriptWithScope"], "title": "TEXT" },
        "col_regex": { "bsonType": ["regex"], "title": "TEXT" },
        "col_minKey": { "bsonType": ["minKey"], "title": "TEXT" },
        "col_maxKey": { "bsonType": ["maxKey"], "title": "TEXT" },
    };

    let options = Validator::new(labels, column_types, Vec::new(), None)
        .unwrap()
        .to_options();

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
        }),
        "col_regex": Bson::RegularExpression(bson::Regex {
            pattern: "^[a-z]*$".to_owned(),
            options: "i".to_owned()
        }),
        "col_minKey": Bson::MinKey,
        "col_maxKey": Bson::MaxKey,
    };

    storage
        .db
        .collection(table_name)
        .insert_one(data, None)
        .await
        .expect("insert_data");

    let mut glue = Glue::new(storage);

    let cases = vec![(
        glue.execute(format! {"SELECT * FROM {table_name}"}).await,
        Ok(Payload::Select {
            labels: vec![
                "col_javascript".to_owned(),
                "col_javascriptWithScope".to_owned(),
                "col_regex".to_owned(),
                "col_minKey".to_owned(),
                "col_maxKey".to_owned(),
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
                Value::Str("/^[a-z]*$/i".to_owned()),
                Value::Str("MinKey()".to_owned()),
                Value::Str("MaxKey()".to_owned()),
            ]],
        }),
    )];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
