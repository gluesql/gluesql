#![cfg(feature = "test-mongo")]

use {
    bson::{Bson, doc},
    gluesql_core::prelude::{Glue, Payload, Value},
    gluesql_mongo_storage::{MongoStorage, utils::Validator},
    std::{collections::BTreeMap, vec},
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
                Value::Map(BTreeMap::from([
                    (
                        "code".to_owned(),
                        Value::Str("function sub(a, b) { return a - b; }".to_owned()),
                    ),
                    (
                        "scope".to_owned(),
                        Value::Map(BTreeMap::from([
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

#[tokio::test]
async fn mongo_float_vector_types() {
    let conn_str = "mongodb://localhost:27017";

    let storage = MongoStorage::new(conn_str, "mongo_float_vector_types")
        .await
        .expect("MongoStorage::new");
    storage.drop_database().await.expect("database dropped");

    let labels = vec![
        "id".to_owned(),
        "vector_col".to_owned(),
    ];
    let column_types = doc! {
        "id": { "bsonType": ["int"], "title": "INT" },
        "vector_col": { "bsonType": ["array"], "title": "FLOAT_VECTOR" },
    };

    let options = Validator::new(labels, column_types, Vec::new(), None)
        .unwrap()
        .to_options();

    let table_name = "float_vector_collection";

    storage
        .db
        .create_collection(table_name, options)
        .await
        .expect("create_collection");

    // Test data with different BSON number types that should convert to FloatVector
    let test_cases = vec![
        doc! {
            "id": 1,
            "vector_col": Bson::Array(vec![
                Bson::Double(1.0),
                Bson::Double(2.0), 
                Bson::Double(3.0)
            ])
        },
        doc! {
            "id": 2, 
            "vector_col": Bson::Array(vec![
                Bson::Int32(1),
                Bson::Double(2.5),
                Bson::Int64(3)
            ])
        },
        doc! {
            "id": 3,
            "vector_col": Bson::Array(vec![
                Bson::Int64(4),
                Bson::Int64(5),
                Bson::Int64(6)
            ])
        },
    ];

    for data in test_cases {
        storage
            .db
            .collection(table_name)
            .insert_one(data, None)
            .await
            .expect("insert_data");
    }

    let mut glue = Glue::new(storage);

    // Test vector function with MongoDB FloatVector data
    let result = glue
        .execute(format!(
            "SELECT id, VECTOR_MAGNITUDE(vector_col) as magnitude FROM {} ORDER BY id",
            table_name
        ))
        .await
        .expect("vector magnitude query failed");

    if let Payload::Select { labels, rows } = &result[0] {
        assert_eq!(labels, &vec!["id".to_owned(), "magnitude".to_owned()]);
        assert_eq!(rows.len(), 3);
        
        // Verify first row (vector [1.0, 2.0, 3.0] has magnitude ~3.74)
        if let (Value::I32(1), Value::F64(magnitude)) = (&rows[0][0], &rows[0][1]) {
            assert!((magnitude - (14.0_f64).sqrt()).abs() < 0.001);
        } else {
            panic!("Expected I32(1) and F64 magnitude for first row");
        }
    } else {
        panic!("Expected Select payload");
    }

    // Test FloatVector insertion and retrieval
    let insert_result = glue
        .execute(format!(
            "INSERT INTO {} VALUES (4, '[7.0, 8.0, 9.0]')",
            table_name
        ))
        .await
        .expect("FloatVector insert failed");

    assert_eq!(insert_result[0], Payload::Insert(1));

    let select_result = glue
        .execute(format!(
            "SELECT vector_col FROM {} WHERE id = 4",
            table_name
        ))
        .await
        .expect("FloatVector select failed");

    if let Payload::Select { rows, .. } = &select_result[0] {
        if let Value::FloatVector(vec) = &rows[0][0] {
            assert_eq!(vec.data(), &[7.0, 8.0, 9.0]);
        } else {
            panic!("Expected FloatVector value");
        }
    } else {
        panic!("Expected Select payload");
    }
}
