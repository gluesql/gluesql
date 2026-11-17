use {
    gluesql_core::{
        data::{Key, Schema, Value},
        store::{AlterTable, Store, StoreMut, Transaction},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Connection, Error as SqliteError, ffi},
    std::collections::BTreeMap,
    tempfile::tempdir,
};

#[test]
fn rename_preserves_metadata_and_rowids_on_reopen() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("rename.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let schema = Schema::from_ddl(
        "CREATE TABLE item (
             id INT PRIMARY KEY,
             level INT8 NOT NULL DEFAULT CAST(7 AS INT8) COMMENT 'level'
         ) COMMENT = 'items'",
    )
    .unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .append_data("item", vec![vec![Value::I64(10), Value::I8(4)]])
        .unwrap();

    // Native identifiers are quoted, while metadata names are bound values.
    storage.rename_schema("ITEM", "renamed\"table").unwrap();
    storage
        .rename_column("renamed\"table", "level", "renamed\"level")
        .unwrap();
    drop(storage);

    let storage = SqliteStorage::new(&path).unwrap();
    let actual = storage.fetch_schema("renamed\"table").unwrap();
    let mut expected = schema;
    expected.table_name = "renamed\"table".to_owned();
    expected.column_defs.as_mut().unwrap()[1].name = "renamed\"level".to_owned();
    assert_eq!(actual, Some(expected));
    assert_eq!(storage.fetch_schema("item").unwrap(), None);

    let sqlite = Connection::open(path).unwrap();
    let actual = sqlite
        .query_row(
            r#"SELECT rowid, id, "renamed""level" FROM "renamed""table""#,
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .unwrap();
    let expected = (1, 10, 4);
    assert_eq!(actual, expected);

    let actual: i64 = sqlite
        .query_row(
            "SELECT count(*) FROM __gluesql_column_meta WHERE table_name = 'item'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(actual, 0);
}

#[test]
fn schemaless_rename_preserves_the_document_marker_on_reopen() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("documents.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    storage
        .insert_schema(&Schema::from_ddl("CREATE TABLE documents").unwrap())
        .unwrap();
    let document = Value::Map(BTreeMap::from([("id".to_owned(), Value::I64(10))]));
    storage
        .append_data("documents", vec![vec![document.clone()]])
        .unwrap();

    storage.rename_schema("documents", "renamed").unwrap();
    drop(storage);

    let storage = SqliteStorage::new(path).unwrap();
    let actual = storage.fetch_schema("renamed").unwrap();
    let expected = Some(Schema::from_ddl("CREATE TABLE renamed").unwrap());
    assert_eq!(actual, expected);

    let actual = storage.fetch_data("renamed", &Key::I64(1)).unwrap();
    let expected = Some(vec![document]);
    assert_eq!(actual, expected);
}

#[test]
fn failed_metadata_rename_preserves_the_table_and_earlier_transaction_writes() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("rename_failure.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let schema = Schema::from_ddl("CREATE TABLE item (id INT) COMMENT = 'original'").unwrap();
    storage.insert_schema(&schema).unwrap();
    let sqlite = Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            "CREATE TRIGGER reject_column_metadata
             BEFORE UPDATE ON __gluesql_column_meta
             BEGIN
                 SELECT RAISE(ABORT, 'metadata is locked');
             END",
        )
        .unwrap();

    storage.begin(false).unwrap();
    storage
        .append_data("item", vec![vec![Value::I64(10)]])
        .unwrap();

    // Physical rename and table metadata succeed before column metadata fails.
    let actual = storage.rename_schema("item", "renamed");
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_TRIGGER),
        Some("metadata is locked".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(schema.clone()));
    assert_eq!(storage.fetch_schema("renamed").unwrap(), None);

    storage.commit().unwrap();
    drop(storage);

    let storage = SqliteStorage::new(path).unwrap();
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(schema));
    let actual = storage.fetch_data("item", &Key::I64(1)).unwrap();
    let expected = Some(vec![Value::I64(10)]);
    assert_eq!(actual, expected);

    let actual: String = sqlite
        .query_row("SELECT table_name FROM __gluesql_table_meta", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(actual, "item");
}
