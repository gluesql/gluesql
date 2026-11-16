use {
    gluesql_core::{
        error::{SchemaParseError, ValueError},
        prelude::{Error, Glue},
    },
    gluesql_json_storage::{JsonStorage, error::JsonStorageError},
    std::path::Path,
};

#[test]
fn json_error() {
    let cases = [
        (
            "wrong-format-jsonl",
            "SELECT * FROM WrongFormatJsonl",
            ValueError::InvalidJsonString("[".to_owned()).into(),
        ),
        (
            "wrong-format-json",
            "SELECT * FROM WrongFormatJson",
            Error::StorageMsg(
                JsonStorageError::InvalidJsonContent("WrongFormatJson.json".to_owned()).to_string(),
            ),
        ),
        (
            "wrong-schema",
            "SELECT * FROM WrongSchema",
            Error::Schema(SchemaParseError::CannotParseDDL),
        ),
        (
            "wrong-table-name",
            "SELECT * FROM WrongTableName",
            Error::StorageMsg(JsonStorageError::TableNameDoesNotMatchWithFile.to_string()),
        ),
        (
            "duplicated",
            "SELECT * FROM Duplicated",
            Error::StorageMsg(
                JsonStorageError::BothJsonlAndJsonExist("Duplicated".to_owned()).to_string(),
            ),
        ),
        (
            "duplicated",
            "DROP TABLE Duplicated",
            Error::StorageMsg(
                JsonStorageError::BothJsonlAndJsonExist("Duplicated".to_owned()).to_string(),
            ),
        ),
        (
            "json-object-type-required",
            "SELECT * FROM JsonObjectTypeRequired",
            Error::StorageMsg(JsonStorageError::JsonObjectTypeRequired.to_string()),
        ),
        (
            "json-array-type-required",
            "SELECT * FROM JsonArrayTypeRequired",
            Error::StorageMsg(JsonStorageError::JsonArrayTypeRequired.to_string()),
        ),
    ];

    let samples = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/samples/errors");

    for (directory, sql, expected) in cases {
        let storage = JsonStorage::new(samples.join(directory)).unwrap();
        let mut glue = Glue::new(storage);
        assert_eq!(glue.execute(sql).unwrap_err(), expected, "{sql}");
    }
}
