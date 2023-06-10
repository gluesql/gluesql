use {
    gluesql_core::prelude::{Glue, Value},
    gluesql_json_storage::JsonStorage,
    serde_json::json,
    test_suite::select_map,
};

#[tokio::test]
async fn json_schemaless() {
    let path = "./tests/samples/";
    let json_storage = JsonStorage::new(path).unwrap();
    let mut glue = Glue::new(json_storage);

    let cases = vec![
        (
            glue.execute("SELECT * FROM Schemaless").await,
            Ok(select_map![
                json!({"id": 1}),
                json!({"name": "Glue"}),
                json!({"id": 3, "name": "SQL"})
            ]),
        ),
        (
            glue.execute("SELECT * FROM ArrayOfJsonsSchemaless").await,
            Ok(select_map![
                json!({ "id": 1, "name": "Glue" }),
                json!({ "id": 2, "name": "SQL" })
            ]),
        ),
        (
            glue.execute("SELECT * FROM SingleJsonSchemaless").await,
            Ok(select_map![json!(
                {
                  "data": [
                    {
                      "id": 1,
                      "name": "Glue"
                    },
                    {
                      "id": 2,
                      "name": "SQL"
                    },
                  ]
                }
            )]),
        ),
    ];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
