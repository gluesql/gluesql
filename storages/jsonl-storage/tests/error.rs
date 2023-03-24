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
            Err(Error::StorageMsg(
                JsonlStorageError::InvalidJsonString(
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
                .to_string(),
            )),
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
        (
            glue.execute("SELECT * FROM Duplicated"),
            Err(Error::StorageMsg(
                JsonlStorageError::BothJsonlAndJsonExist("Duplicated".to_owned()).to_string(),
            )),
        ),
        (
            glue.execute("DROP TABLE Duplicated"),
            Err(Error::StorageMsg(
                JsonlStorageError::BothJsonlAndJsonExist("Duplicated".to_owned()).to_string(),
            )),
        ),
        (
            glue.execute("SELECT * FROM JsonObjectTypeRequired"),
            Err(Error::StorageMsg(
                JsonlStorageError::JsonObjectTypeRequired.to_string(),
            )),
        ),
        (
            glue.execute("SELECT * FROM JsonArrayTypeRequired"),
            Err(Error::StorageMsg(
                JsonlStorageError::JsonArrayTypeRequired.to_string(),
            )),
        ),
    ];

    for (actual, expected) in cases {
        test(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
