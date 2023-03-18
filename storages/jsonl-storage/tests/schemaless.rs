use {
    gluesql_core::prelude::{Glue, Payload, Value},
    gluesql_jsonl_storage::JsonlStorage,
    serde_json::json,
    test_suite::{select_map, test},
};

#[test]
fn jsonl_schemaless() {
    let path = "./tests/samples/";
    let jsonl_storage = JsonlStorage::new(path).unwrap();
    let mut glue = Glue::new(jsonl_storage);

    let cases = vec![
        (
            glue.execute("SELECT * FROM Schemaless"),
            Ok(select_map![
                json!({"id": 1}),
                json!({"name": "Glue"}),
                json!({"id": 3, "name": "SQL"})
            ]),
        ),
        (
            glue.execute("SELECT * FROM ArrayOfJsonsSchemaless"),
            Ok(select_map![
                json!({ "id": 1, "name": "Glue" }),
                json!({ "id": 2, "name": "SQL" })
            ]),
        ),
        (
            glue.execute("SELECT * FROM SingleJsonSchemaless"),
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
        (
            glue.execute(r#"INSERT INTO JsonDML VALUES ('{"id": 2, "notice": "appended json"}')"#),
            Ok(Payload::Insert(1)),
        ),
        (
            glue.execute("SELECT * FROM JsonDML"),
            Ok(select_map![
                json!({
                  "id": 1,
                  "notice": "should keep this array of jsons format"
                }),
                json!({
                  "id": 2,
                  "notice": "appended json"
                })
            ]),
        ),
        (
            glue.execute("UPDATE JsonDML SET notice = 'updated' WHERE id = 2"),
            Ok(Payload::Update(1)),
        ),
        (
            glue.execute("SELECT * FROM JsonDML WHERE id = 2"),
            Ok(select_map![json!({
              "id": 2,
              "notice": "updated"
            })]),
        ),
        (
            glue.execute("DELETE FROM JsonDML WHERE id = 2"),
            Ok(Payload::Delete(1)),
        ),
        (
            glue.execute("SELECT * FROM JsonDML"),
            Ok(select_map![json!({
              "id": 1,
              "notice": "should keep this array of jsons format"
            })]),
        ),
    ];

    for (actual, expected) in cases {
        test(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
