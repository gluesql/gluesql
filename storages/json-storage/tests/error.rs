use {
    gluesql_core::{
        error::{SchemaParseError, ValueError},
        prelude::{Error, Glue},
    },
    gluesql_json_storage::{error::JsonStorageError, JsonStorage},
};

#[tokio::test]
async fn json_error() {
    let path = "./tests/samples/";
    let json_storage = JsonStorage::new(path).unwrap();
    let mut glue = Glue::new(json_storage);

    let cases = vec![
        (
            glue.execute("SELECT * FROM WrongFormatJsonl").await,
            Err(ValueError::InvalidJsonString("[".to_owned()).into()),
        ),
        (
            glue.execute("SELECT * FROM WrongFormatJson").await,
            Err(Error::StorageMsg(
                JsonStorageError::InvalidJsonContent("WrongFormatJson.json".to_owned()).to_string(),
            )),
        ),
        (
            glue.execute("SELECT * FROM WrongSchema").await,
            Err(Error::Schema(SchemaParseError::CannotParseDDL)),
        ),
        (
            glue.execute("SELECT * FROM WrongTableName").await,
            Err(Error::StorageMsg(
                JsonStorageError::TableNameDoesNotMatchWithFile.to_string(),
            )),
        ),
        (
            glue.execute("SELECT * FROM Duplicated").await,
            Err(Error::StorageMsg(
                JsonStorageError::BothJsonlAndJsonExist("Duplicated".to_owned()).to_string(),
            )),
        ),
        (
            glue.execute("DROP TABLE Duplicated").await,
            Err(Error::StorageMsg(
                JsonStorageError::BothJsonlAndJsonExist("Duplicated".to_owned()).to_string(),
            )),
        ),
        (
            glue.execute("SELECT * FROM JsonObjectTypeRequired").await,
            Err(Error::StorageMsg(
                JsonStorageError::JsonObjectTypeRequired.to_string(),
            )),
        ),
        (
            glue.execute("SELECT * FROM JsonArrayTypeRequired").await,
            Err(Error::StorageMsg(
                JsonStorageError::JsonArrayTypeRequired.to_string(),
            )),
        ),
    ];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
