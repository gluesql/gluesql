use {
    gluesql_core::{
        ast::DataType,
        data::{Key, Schema, Value},
        prelude::Glue,
        store::{NativeQuery, Store, StoreMut},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Connection, Error as SqliteError, ffi},
    tempfile::tempdir,
};

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

#[test]
fn parent_column_rebuilds_preserve_references_and_restore_immediate_checks() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("parent_rebuild.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "CREATE TABLE parent (id INT PRIMARY KEY, old_col TEXT, code INT UNIQUE);
         CREATE TABLE child (
             parent_id INT,
             CONSTRAINT parent_fk FOREIGN KEY (parent_id) REFERENCES parent (id)
         );
         INSERT INTO parent VALUES (10, 'ten', 100);
         INSERT INTO child VALUES (10)",
    )
    .unwrap();

    glue.execute(
        "BEGIN;
         INSERT INTO parent VALUES (20, 'twenty', 200);
         INSERT INTO child VALUES (20);
         ALTER TABLE parent ADD COLUMN extra TEXT;
         ALTER TABLE parent ADD COLUMN level INT8 NOT NULL DEFAULT CAST(7 AS INT8);
         ALTER TABLE parent DROP COLUMN old_col;
         ALTER TABLE parent DROP COLUMN code",
    )
    .unwrap();

    let actual = glue
        .storage
        .query_native("PRAGMA defer_foreign_keys", &[], &[DataType::Int])
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected = vec![vec![Value::I64(0)]];
    assert_eq!(actual, expected);

    // Call storage directly so SQLite, rather than the executor, rejects the orphan.
    let actual = glue
        .storage
        .append_data("child", vec![vec![Value::I64(99)]]);
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_FOREIGNKEY),
        Some("FOREIGN KEY constraint failed".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);
    glue.execute("COMMIT").unwrap();
    drop(glue);

    let storage = SqliteStorage::new(&path).unwrap();
    let actual = storage.fetch_schema("parent").unwrap();
    let expected = Some(
        Schema::from_ddl(
            "CREATE TABLE parent (
                 id INT PRIMARY KEY,
                 extra TEXT,
                 level INT8 NOT NULL DEFAULT CAST(7 AS INT8)
             )",
        )
        .unwrap(),
    );
    assert_eq!(actual, expected);

    let actual = storage.fetch_data("parent", &Key::I64(20)).unwrap();
    let expected = Some(vec![Value::I64(20), Value::Null, Value::I8(7)]);
    assert_eq!(actual, expected);

    let sqlite = Connection::open(path).unwrap();
    let actual = sqlite
        .prepare(
            "SELECT p.rowid, p.id, c.rowid, c.parent_id
             FROM parent AS p JOIN child AS c ON c.parent_id = p.id
             ORDER BY p.rowid",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let expected = vec![(1, 10, 1, 10), (2, 20, 2, 20)];
    assert_eq!(actual, expected);
}

#[test]
fn column_rebuilds_preserve_self_references_regardless_of_copy_order() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "CREATE TABLE node (
             id INT PRIMARY KEY,
             parent_id INT,
             CONSTRAINT parent_fk FOREIGN KEY (parent_id) REFERENCES node (id)
         );
         INSERT INTO node VALUES (1, NULL);
         INSERT INTO node VALUES (2, 1);
         UPDATE node SET parent_id = 2 WHERE id = 1",
    )
    .unwrap();

    // The first copied row references the second, which has not been copied yet.
    glue.execute("ALTER TABLE node ADD COLUMN label TEXT")
        .unwrap();

    let actual = glue
        .storage
        .query_native(
            "SELECT rowid, id, parent_id, label FROM node ORDER BY rowid",
            &[],
            &[DataType::Int, DataType::Int, DataType::Int, DataType::Text],
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected = vec![
        vec![Value::I64(1), Value::I64(1), Value::I64(2), Value::Null],
        vec![Value::I64(2), Value::I64(2), Value::I64(1), Value::Null],
    ];
    assert_eq!(actual, expected);
}

#[test]
fn schema_replacement_rejects_broken_references_before_restoring_deferral() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "CREATE TABLE parent (id INT PRIMARY KEY);
         CREATE TABLE other_parent (id INT PRIMARY KEY);
         CREATE TABLE child (
             parent_id INT,
             CONSTRAINT parent_fk FOREIGN KEY (parent_id) REFERENCES parent (id)
         );
         INSERT INTO parent VALUES (10);
         INSERT INTO other_parent VALUES (20);
         INSERT INTO child VALUES (10)",
    )
    .unwrap();

    // Direct schema replacement can invalidate a reference that SQL ALTER preserves.
    let replacement = Schema::from_ddl(
        "CREATE TABLE child (
             parent_id INT,
             CONSTRAINT parent_fk FOREIGN KEY (parent_id) REFERENCES other_parent (id)
         )",
    )
    .unwrap();
    let actual = glue.storage.insert_schema(&replacement);
    let expected = Err(SqliteStorageError::ConflictForeignKey {
        table: "child".to_owned(),
        rowid: 1,
        parent: "other_parent".to_owned(),
    }
    .into());
    assert_eq!(actual, expected);

    let actual = glue.storage.fetch_schema("child").unwrap();
    let expected = Some(
        Schema::from_ddl(
            "CREATE TABLE child (
                 parent_id INT,
                 CONSTRAINT parent_fk FOREIGN KEY (parent_id) REFERENCES parent (id)
             )",
        )
        .unwrap(),
    );
    assert_eq!(actual, expected);

    let actual = glue.storage.fetch_data("child", &Key::I64(1)).unwrap();
    let expected = Some(vec![Value::I64(10)]);
    assert_eq!(actual, expected);
}
