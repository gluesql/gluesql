use {
    gluesql_core::{
        data::{SchemaParseError, ValueError},
        prelude::Glue,
        result::Error,
    },
    gluesql_jsonl_storage::JsonlStorage,
    test_suite::test,
};

#[test]
fn jsonl_primary_key() {
    let path = "./tests/samples/";
    let jsonl_storage = JsonlStorage::new(path).unwrap();
    let mut glue = Glue::new(jsonl_storage);

    let cases = vec![
        (
            glue.execute("SELECT * FROM WrongFormat"),
            Err(ValueError::InvalidJsonString("{".to_owned()).into()),
        ),
        (
            glue.execute("SELECT * FROM WrongSchema"),
            Err(Error::Schema(SchemaParseError::CannotParseDDL)),
        ),
    ];

    for (actual, expected) in cases {
        test(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
