use {
    gluesql_core::prelude::Glue,
    gluesql_csv_storage::{CsvStorage, error::CsvStorageError},
};

#[tokio::test]
async fn error() {
    let path = "./tests/samples/";
    let storage = CsvStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    let actual = glue.execute("SELECT * FROM WrongSchemaName").await;
    let expected = Err(CsvStorageError::TableNameDoesNotMatchWithFile.into());
    assert_eq!(actual, expected);
}
