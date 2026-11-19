use {
    gluesql_core::{
        data::{Key, Schema, Value},
        store::{Store, StoreMut},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Connection, Error as SqliteError, ffi},
    tempfile::tempdir,
};

#[test]
fn schema_rebuild_preserves_rows_and_rolls_back_failed_replacement() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("schema.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();

    let initial = Schema::from_ddl("CREATE TABLE item (id INT PRIMARY KEY, name TEXT)").unwrap();
    storage.insert_schema(&initial).unwrap();
    storage
        .insert_data(
            "item",
            vec![(
                Key::I64(4),
                vec![Value::I64(4), Value::Str("apple".to_owned())],
            )],
        )
        .unwrap();

    let expanded = Schema::from_ddl(
        "CREATE TABLE item (
             id INT PRIMARY KEY,
             name TEXT,
             qty INT NOT NULL DEFAULT 3 COMMENT 'new'
         ) COMMENT = 'expanded'",
    )
    .unwrap();
    storage.insert_schema(&expanded).unwrap();

    assert_eq!(
        storage.fetch_data("item", &Key::I64(4)).unwrap(),
        Some(vec![
            Value::I64(4),
            Value::Str("apple".to_owned()),
            Value::I64(3)
        ])
    );
    assert_eq!(
        storage.fetch_schema("item").unwrap(),
        Some(expanded.clone())
    );

    // Existing rows cannot populate a required column without a default.
    let invalid = Schema::from_ddl(
        "CREATE TABLE item (
             id INT PRIMARY KEY,
             name TEXT,
             qty INT NOT NULL DEFAULT 3 COMMENT 'new',
             required INT NOT NULL COMMENT 'new'
         ) COMMENT = 'expanded'",
    )
    .unwrap();

    let actual = storage.insert_schema(&invalid);
    let expected = SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_NOTNULL),
        Some("NOT NULL constraint failed: item.required".to_owned()),
    ));
    assert_eq!(actual, Err(expected.into()));

    // Reopening must expose the last successful schema and its rows.
    drop(storage);
    let storage = SqliteStorage::new(&path).unwrap();
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(expanded));
    assert_eq!(
        storage.fetch_data("item", &Key::I64(4)).unwrap(),
        Some(vec![
            Value::I64(4),
            Value::Str("apple".to_owned()),
            Value::I64(3)
        ])
    );
}

#[test]
fn rewriting_schema_replaces_column_metadata_and_removes_unused_table_metadata() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("metadata.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let original = Schema::from_ddl(
        "CREATE TABLE item (
             id INT PRIMARY KEY,
             level INT8 DEFAULT CAST(1 AS INT8) COMMENT 'old',
             obsolete TEXT
         ) COMMENT = 'original'",
    )
    .unwrap();
    storage.insert_schema(&original).unwrap();

    // Replace the logical type, remove an expression/comment and drop an entire column.
    let updated = Schema::from_ddl(
        "CREATE TABLE item (
             id INT PRIMARY KEY,
             level INT16 DEFAULT 2
         ) COMMENT = 'updated'",
    )
    .unwrap();
    storage.insert_schema(&updated).unwrap();

    assert_eq!(storage.fetch_schema("item").unwrap(), Some(updated));
    assert_eq!(storage.fetch_schema("ITEM").unwrap(), None);

    // Old metadata must be deleted, including the row for the dropped column.
    let sqlite = Connection::open(&path).unwrap();
    let actual = sqlite
        .prepare(
            "SELECT column_name, logical_type, comment, default_sql
             FROM __gluesql_column_meta
             WHERE table_name = 'item'
             ORDER BY column_name",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let expected = vec![
        ("id".to_owned(), "INT".to_owned(), None, None),
        ("level".to_owned(), "INT16".to_owned(), None, None),
    ];
    assert_eq!(actual, expected);

    // A schemaful table no longer needs a table metadata row once its comment is removed.
    let without_comment = Schema::from_ddl(
        "CREATE TABLE item (
             id INT PRIMARY KEY,
             level INT16 DEFAULT 2
         )",
    )
    .unwrap();
    storage.insert_schema(&without_comment).unwrap();
    let actual: i64 = sqlite
        .query_row(
            "SELECT count(*) FROM __gluesql_table_meta WHERE table_name = 'item'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let expected = 0;
    assert_eq!(actual, expected);

    let actual = storage.fetch_schema("item").unwrap();
    let expected = Some(without_comment);
    assert_eq!(actual, expected);
}

#[test]
fn failed_table_comment_changes_restore_the_previous_schema_and_rows() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("table-comment.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let original =
        Schema::from_ddl("CREATE TABLE item (id INT PRIMARY KEY) COMMENT = 'original'").unwrap();
    storage.insert_schema(&original).unwrap();
    storage
        .append_data("item", vec![vec![Value::I64(7)]])
        .unwrap();

    let mut replacement = Schema::from_ddl(
        "CREATE TABLE item (id INT PRIMARY KEY, qty INT DEFAULT 3) COMMENT = 'updated'",
    )
    .unwrap();
    let sqlite = Connection::open(&path).unwrap();

    reject_metadata_write(&sqlite, "UPDATE ON __gluesql_table_meta");
    let actual = storage.insert_schema(&replacement);
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_TRIGGER),
        Some("metadata is read-only".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);
    assert_eq!(
        storage.fetch_schema("item").unwrap(),
        Some(original.clone())
    );
    assert_eq!(
        storage.fetch_data("item", &Key::I64(7)).unwrap(),
        Some(vec![Value::I64(7)])
    );

    replacement.comment = None;
    reject_metadata_write(&sqlite, "DELETE ON __gluesql_table_meta");
    let actual = storage.insert_schema(&replacement);
    assert_eq!(actual, expected);
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(original));
    assert_eq!(
        storage.fetch_data("item", &Key::I64(7)).unwrap(),
        Some(vec![Value::I64(7)])
    );
}

#[test]
fn failed_column_metadata_replacement_restores_the_previous_schema_and_rows() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("column-metadata.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let original = Schema::from_ddl(
        "CREATE TABLE item (id INT PRIMARY KEY COMMENT 'original id') COMMENT = 'original'",
    )
    .unwrap();
    storage.insert_schema(&original).unwrap();
    storage
        .append_data("item", vec![vec![Value::I64(7)]])
        .unwrap();

    let replacement =
        Schema::from_ddl("CREATE TABLE item (id INT PRIMARY KEY, qty INT DEFAULT 3)").unwrap();
    let sqlite = Connection::open(&path).unwrap();

    reject_metadata_write(&sqlite, "DELETE ON __gluesql_column_meta");
    let actual = storage.insert_schema(&replacement);
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_TRIGGER),
        Some("metadata is read-only".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);
    assert_eq!(
        storage.fetch_schema("item").unwrap(),
        Some(original.clone())
    );
    assert_eq!(
        storage.fetch_data("item", &Key::I64(7)).unwrap(),
        Some(vec![Value::I64(7)])
    );

    // The id metadata has already been replaced when SQLite rejects the new column.
    reject_metadata_write(
        &sqlite,
        "INSERT ON __gluesql_column_meta WHEN NEW.column_name = 'qty'",
    );
    let actual = storage.insert_schema(&replacement);
    assert_eq!(actual, expected);
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(original));
    assert_eq!(
        storage.fetch_data("item", &Key::I64(7)).unwrap(),
        Some(vec![Value::I64(7)])
    );
}

#[test]
fn failed_metadata_cleanup_restores_the_dropped_table_and_its_rows() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("drop.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let schema =
        Schema::from_ddl("CREATE TABLE item (id INT PRIMARY KEY) COMMENT = 'items'").unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .append_data("item", vec![vec![Value::I64(7)]])
        .unwrap();
    let sqlite = Connection::open(&path).unwrap();

    reject_metadata_write(&sqlite, "DELETE ON __gluesql_column_meta");
    let actual = storage.delete_schema("item");
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_TRIGGER),
        Some("metadata is read-only".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(schema.clone()));

    reject_metadata_write(&sqlite, "DELETE ON __gluesql_table_meta");
    let actual = storage.delete_schema("item");
    assert_eq!(actual, expected);
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(schema));
    assert_eq!(
        storage.fetch_data("item", &Key::I64(7)).unwrap(),
        Some(vec![Value::I64(7)])
    );
}

fn reject_metadata_write(sqlite: &Connection, event: &str) {
    sqlite
        .execute_batch(&format!(
            "DROP TRIGGER IF EXISTS reject_metadata_write;
             CREATE TRIGGER reject_metadata_write BEFORE {event}
             BEGIN SELECT RAISE(ABORT, 'metadata is read-only'); END"
        ))
        .unwrap();
}
