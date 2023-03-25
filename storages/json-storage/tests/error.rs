use {
    gluesql_core::{
        data::{SchemaParseError, ValueError},
        prelude::Glue,
        result::Error,
    },
    gluesql_json_storage::{error::JsonStorageError, JsonStorage},
    test_suite::test,
};

#[test]
fn json_error() {
    let path = "./tests/samples/";
    let json_storage = JsonStorage::new(path).unwrap();
    let mut glue = Glue::new(json_storage);

    let cases = vec![
        (
            glue.execute("SELECT * FROM WrongFormatJsonl"),
            Err(ValueError::InvalidJsonString("[".to_owned()).into()),
        ),
        (
            glue.execute("SELECT * FROM WrongFormatJson"),
            Err(Error::StorageMsg(
                JsonStorageError::InvalidJsonString(
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
                JsonStorageError::TableNameDoesNotMatchWithFile.to_string(),
            )),
        ),
        (
            glue.execute("SELECT * FROM Duplicated"),
            Err(Error::StorageMsg(
                JsonStorageError::BothJsonlAndJsonExist("Duplicated".to_owned()).to_string(),
            )),
        ),
        (
            glue.execute("DROP TABLE Duplicated"),
            Err(Error::StorageMsg(
                JsonStorageError::BothJsonlAndJsonExist("Duplicated".to_owned()).to_string(),
            )),
        ),
        (
            glue.execute("SELECT * FROM JsonObjectTypeRequired"),
            Err(Error::StorageMsg(
                JsonStorageError::JsonObjectTypeRequired.to_string(),
            )),
        ),
        (
            glue.execute("SELECT * FROM JsonArrayTypeRequired"),
            Err(Error::StorageMsg(
                JsonStorageError::JsonArrayTypeRequired.to_string(),
            )),
        ),
    ];

    for (actual, expected) in cases {
        test(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
