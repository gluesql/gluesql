#![cfg(feature = "test-mongo")]

use {
    gluesql_core::prelude::{Glue, Payload, Value},
    gluesql_mongo_storage::MongoStorage,
    serde_json::json,
    std::collections::HashMap,
};

#[tokio::test]
async fn mongo_array_schemaless() {
    let conn_str = "mongodb://localhost:27017";

    let storage = MongoStorage::new(conn_str, "mongo_array")
        .await
        .expect("MongoStorage::new");
    storage.drop_database().await.expect("database dropped");

    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Logs").await.unwrap();
    glue.execute(
        format!(
            "INSERT INTO Logs VALUES ('{}'), ('{}');",
            json!({ "id": 1, "value": 30 }),
            json!({ "id": 2, "rate": 3.5, "list": [1, 2, 3] })
        )
        .as_str(),
    )
    .await
    .unwrap();

    let cases = vec![(
        glue.execute("SELECT * FROM Logs").await,
        Ok(Payload::SelectMap(vec![
            HashMap::from([
                ("id".to_owned(), Value::I64(1)),
                ("value".to_owned(), Value::I64(30)),
            ]),
            HashMap::from([
                ("id".to_owned(), Value::I64(2)),
                ("rate".to_owned(), Value::F64(3.5)),
                (
                    "list".to_owned(),
                    Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]),
                ),
            ]),
        ])),
    )];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
