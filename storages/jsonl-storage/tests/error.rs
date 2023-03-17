use {
    gluesql_core::{
        data::{SchemaParseError, ValueError},
        prelude::Glue,
        result::Error,
    },
    gluesql_jsonl_storage::{error::JsonlStorageError, JsonlStorage},
    test_suite::test,
};

#[test]
fn jsonl_error() {
    let path = "./tests/samples/";
    let jsonl_storage = JsonlStorage::new(path).unwrap();
    let mut glue = Glue::new(jsonl_storage);

    let cases = vec![
        (
            glue.execute("SELECT * FROM WrongFormatJsonl"),
            Err(ValueError::InvalidJsonString("[".to_owned()).into()),
        ),
        (
            glue.execute("SELECT * FROM WrongFormatJson"),
            Err(ValueError::InvalidJsonString(
                r#"{
  "id": 1,
  "notice": "*.json usage1: An array of jsons"
},
{
  "id": 2,
  "notice": "*.json usage2: A single json in a file"
}
"#
                .to_owned(),
            )
            .into()),
        ),
        (
            glue.execute("SELECT * FROM WrongSchema"),
            Err(Error::Schema(SchemaParseError::CannotParseDDL)),
        ),
        (
            glue.execute("SELECT * FROM WrongTableName"),
            Err(Error::StorageMsg(
                JsonlStorageError::TableNameDoesNotMatchWithFile.to_string(),
            )),
        ),
    ];

    for (actual, expected) in cases {
        test(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
