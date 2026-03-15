use {
    gluesql_core::{
        data::{Key, Value},
        prelude::Glue,
        store::StoreMut,
    },
    gluesql_csv_storage::{CsvStorage, error::CsvStorageError},
    std::{
        fs::remove_dir_all,
        time::{SystemTime, UNIX_EPOCH},
    },
};

#[tokio::test]
async fn wrong_schema_name_returns_table_name_mismatch_error() {
    let path = "./tests/samples/";
    let storage = CsvStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    let err = glue
        .execute("SELECT * FROM WrongSchemaName")
        .await
        .expect_err("schema file table name should match requested table");
    let expected = CsvStorageError::TableNameDoesNotMatchWithFile;
    assert_eq!(err, expected.into());
}

#[tokio::test]
async fn append_schemaless_non_map_row_returns_error() {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = format!("./tests/tmp/append-schemaless-non-map-row-{suffix}");
    let storage = CsvStorage::new(&path).unwrap();
    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Foo").await.unwrap();

    let err = glue
        .storage
        .append_data("Foo", vec![vec![Value::I64(1)]])
        .await
        .expect_err("schemaless row must be map-shaped");
    let expected = CsvStorageError::UnexpectedNonMapRowForSchemalessTable;
    assert_eq!(err, expected.into());

    let _ = remove_dir_all(path);
}

#[tokio::test]
async fn insert_schemaless_non_map_row_returns_error() {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = format!("./tests/tmp/insert-schemaless-non-map-row-{suffix}");
    let storage = CsvStorage::new(&path).unwrap();
    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Foo").await.unwrap();

    let err = glue
        .storage
        .insert_data("Foo", vec![(Key::I64(1), vec![Value::I64(1)])])
        .await
        .expect_err("schemaless row must be map-shaped");
    let expected = CsvStorageError::UnexpectedNonMapRowForSchemalessTable;
    assert_eq!(err, expected.into());

    let _ = remove_dir_all(path);
}
