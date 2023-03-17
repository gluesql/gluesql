use {
    gluesql_core::prelude::{Glue, Payload, Value},
    gluesql_jsonl_storage::JsonlStorage,
};

#[test]
fn jsonl_schemaless() {
    let path = "./tests/samples/";
    let jsonl_storage = JsonlStorage::new(path).unwrap();
    let mut glue = Glue::new(jsonl_storage);

    let actual = glue
        .execute("SELECT * FROM Schemaless")
        .map(|mut payloads| payloads.remove(0));
    let expected = Ok(Payload::SelectMap(vec![
        [("id".to_owned(), Value::I64(1))].into_iter().collect(),
        [("name".to_owned(), Value::Str("Glue".to_owned()))]
            .into_iter()
            .collect(),
        [
            ("id".to_owned(), Value::I64(3)),
            ("name".to_owned(), Value::Str("SQL".to_owned())),
        ]
        .into_iter()
        .collect(),
    ]));

    assert_eq!(actual, expected);
}
