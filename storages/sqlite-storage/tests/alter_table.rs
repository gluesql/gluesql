use {
    gluesql_core::{
        ast::ColumnDef,
        data::{Key, Schema, Value},
        error::{AlterTableError, Error},
        parse_sql::parse_column_def,
        prelude::Glue,
        store::{AlterTable, Store, StoreMut, Transaction},
        translate::translate_column_def,
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Connection, Error as SqliteError, ffi},
    std::collections::BTreeMap,
    tempfile::tempdir,
};

fn column(sql: &str) -> ColumnDef {
    translate_column_def(&parse_column_def(sql).unwrap(), &[]).unwrap()
}

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
fn column_rebuilds_preserve_rowids_and_encoded_defaults() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("columns.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "CREATE TABLE item (_rowid_ INT, id DECIMAL PRIMARY KEY, code INT UNIQUE);
         INSERT INTO item VALUES (10, CAST('1.00' AS DECIMAL), 20)",
    )
    .unwrap();

    // Keep a physical rowid unrelated to the logical primary key or row count.
    let sqlite = Connection::open(&path).unwrap();
    sqlite.execute("UPDATE item SET rowid = 41", []).unwrap();

    // Adding rowid changes the usable alias to oid; defaults need logical encoding.
    glue.execute(
        "ALTER TABLE item ADD COLUMN rowid INT8 NOT NULL DEFAULT CAST(7 AS INT8);
         ALTER TABLE item ADD COLUMN amount DECIMAL NOT NULL DEFAULT CAST('1.2300' AS DECIMAL);
         ALTER TABLE item DROP COLUMN id;
         ALTER TABLE item DROP COLUMN code",
    )
    .unwrap();
    drop(glue);

    let storage = SqliteStorage::new(&path).unwrap();
    let actual = storage.fetch_schema("item").unwrap();
    let expected = Some(
        Schema::from_ddl(
            "CREATE TABLE item (
                 _rowid_ INT,
                 rowid INT8 NOT NULL DEFAULT CAST(7 AS INT8),
                 amount DECIMAL NOT NULL DEFAULT CAST('1.2300' AS DECIMAL)
             )",
        )
        .unwrap(),
    );
    assert_eq!(actual, expected);

    let actual = storage.fetch_data("item", &Key::I64(41)).unwrap();
    let expected = Some(vec![
        Value::I64(10),
        Value::I8(7),
        Value::Decimal("1.2300".parse().unwrap()),
    ]);
    assert_eq!(actual, expected);

    let actual = sqlite
        .query_row("SELECT oid, _rowid_, rowid, amount FROM item", [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .unwrap();
    let expected = (41, 10, 7, "1.2300".to_owned());
    assert_eq!(actual, expected);

    let actual = sqlite
        .prepare("SELECT column_name FROM __gluesql_column_meta ORDER BY column_name")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let expected = vec!["_rowid_", "amount", "rowid"];
    assert_eq!(actual, expected);
}

#[test]
fn failed_column_copy_restores_the_original_table() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("copy_failure.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let schema =
        Schema::from_ddl("CREATE TABLE item (id INT PRIMARY KEY) COMMENT = 'original'").unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .append_data("item", vec![vec![Value::I64(10)], vec![Value::I64(20)]])
        .unwrap();

    // The first copied row succeeds; the second violates the new UNIQUE constraint.
    let actual = storage.add_column("item", &column("code INT UNIQUE DEFAULT 1"), Value::I64(1));
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_UNIQUE),
        Some("UNIQUE constraint failed: item.code".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);
    drop(storage);

    let storage = SqliteStorage::new(&path).unwrap();
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(schema));

    let sqlite = Connection::open(path).unwrap();
    let actual = sqlite
        .prepare("SELECT rowid, id FROM item ORDER BY rowid")
        .unwrap()
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let expected = vec![(1, 10), (2, 20)];
    assert_eq!(actual, expected);

    let actual: i64 = sqlite
        .query_row(
            "SELECT count(*) FROM __gluesql_column_meta WHERE column_name = 'code'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(actual, 0);
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

#[test]
fn unsupported_layout_changes_leave_the_table_unchanged() {
    let mut storage = SqliteStorage::in_memory().unwrap();
    let schema = Schema::from_ddl("CREATE TABLE item (_rowid_ INT, rowid INT, value INT)").unwrap();
    storage.insert_schema(&schema).unwrap();
    let row = vec![Value::I64(10), Value::I64(20), Value::I64(30)];
    storage.append_data("item", vec![row.clone()]).unwrap();

    // Neither adding nor renaming a column may hide the final rowid alias.
    let actual = storage.add_column("item", &column("oid INT"), Value::Null);
    let expected = Err(SqliteStorageError::RowidUnavailable("item".to_owned()).into());
    assert_eq!(actual, expected);

    let actual = storage.rename_column("item", "value", "oid");
    assert_eq!(actual, expected);

    let actual = storage.rename_schema("item", "__GLUESQL_TABLE_META");
    let expected =
        Err(SqliteStorageError::ReservedTableName("__GLUESQL_TABLE_META".to_owned()).into());
    assert_eq!(actual, expected);
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(schema));
    assert_eq!(storage.fetch_data("item", &Key::I64(1)).unwrap(), Some(row));

    // Removing the last column would require a different physical table layout.
    let schema = Schema::from_ddl("CREATE TABLE last_column (id INT)").unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .append_data("last_column", vec![vec![Value::I64(10)]])
        .unwrap();

    let actual = storage.drop_column("last_column", "id", false);
    let expected = Err(SqliteStorageError::EmptySchema("last_column".to_owned()).into());
    assert_eq!(actual, expected);
    assert_eq!(storage.fetch_schema("last_column").unwrap(), Some(schema));
    assert_eq!(
        storage.fetch_data("last_column", &Key::I64(1)).unwrap(),
        Some(vec![Value::I64(10)])
    );
}

#[test]
fn direct_alter_calls_preserve_missing_table_errors() {
    let mut storage = SqliteStorage::in_memory().unwrap();
    let expected = Err(Error::AlterTable(AlterTableError::TableNotFound(
        "absent".to_owned(),
    )));

    assert_eq!(storage.rename_schema("absent", "renamed"), expected);
    assert_eq!(storage.rename_column("absent", "id", "renamed"), expected);
    assert_eq!(
        storage.add_column("absent", &column("id INT"), Value::Null),
        expected
    );
    assert_eq!(storage.drop_column("absent", "id", true), expected);
}

#[test]
fn table_rename_preserves_sqlite_foreign_key_references_on_reopen() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("references.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "CREATE TABLE parent (id INT PRIMARY KEY);
         CREATE TABLE child (
             parent_id INT,
             CONSTRAINT parent_fk FOREIGN KEY (parent_id) REFERENCES parent (id)
         );
         INSERT INTO parent VALUES (10);
         INSERT INTO child VALUES (10);
         ALTER TABLE parent RENAME TO renamed",
    )
    .unwrap();
    drop(glue);

    let storage = SqliteStorage::new(path).unwrap();
    let actual = storage.fetch_schema("child").unwrap();
    let expected = Some(
        Schema::from_ddl(
            "CREATE TABLE child (
                 parent_id INT,
                 CONSTRAINT parent_fk FOREIGN KEY (parent_id) REFERENCES renamed (id)
             )",
        )
        .unwrap(),
    );
    assert_eq!(actual, expected);

    let actual = storage.fetch_data("renamed", &Key::I64(10)).unwrap();
    let expected = Some(vec![Value::I64(10)]);
    assert_eq!(actual, expected);
}
