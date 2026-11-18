use {
    gluesql_core::{
        data::{Key, Value},
        prelude::{Glue, Payload},
        store::StoreMut,
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Connection, Error as SqliteError, ffi},
    tempfile::tempdir,
};

#[test]
fn self_referencing_rows_can_be_deleted_together() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE node (
            id INT PRIMARY KEY,
            parent_id INT,
            FOREIGN KEY (parent_id) REFERENCES node (id)
        );
        INSERT INTO node VALUES (1, NULL), (3, NULL);
        INSERT INTO node VALUES (2, 1);
        ",
    )
    .unwrap();

    let actual = glue.execute("DELETE FROM node WHERE id = 1");
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_FOREIGNKEY),
        Some("FOREIGN KEY constraint failed".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);

    let actual = glue
        .execute(
            "DELETE FROM node WHERE id IN (1, 2);
             SELECT id FROM node",
        )
        .unwrap();
    let expected = vec![
        Payload::Delete(2),
        Payload::Select {
            labels: vec!["id".to_owned()],
            rows: vec![vec![Value::I64(3)]],
        },
    ];
    assert_eq!(actual, expected);
}

#[test]
fn large_key_batches_delete_mutually_referencing_rows_together() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE node (
            id INT PRIMARY KEY,
            peer_id INT,
            FOREIGN KEY (peer_id) REFERENCES node (id)
        );
        INSERT INTO node VALUES (1, NULL);
        INSERT INTO node VALUES (40000, 1);
        UPDATE node SET peer_id = 40000 WHERE id = 1;
        ",
    )
    .unwrap();

    // The two peers are farther apart than SQLite's per-statement parameter limit.
    let keys = (1..=40_000).map(Key::I64).collect();
    glue.storage.delete_data("node", keys).unwrap();

    let actual = glue.execute("SELECT id FROM node").unwrap();
    let expected = vec![Payload::Select {
        labels: vec!["id".to_owned()],
        rows: vec![],
    }];
    assert_eq!(actual, expected);
}

#[test]
fn deletion_only_touches_rows_with_matching_primary_keys() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("delete.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "CREATE TABLE rarray (id INT PRIMARY KEY, flag BOOLEAN);
         INSERT INTO rarray VALUES (1, FALSE), (2, TRUE)",
    )
    .unwrap();

    // A full scan would try to decode this unrelated row's invalid BOOLEAN.
    let sqlite = Connection::open(path).unwrap();
    sqlite
        .execute("UPDATE rarray SET flag = 2 WHERE id = 1", [])
        .unwrap();

    let keys = vec![Key::Str("1".to_owned()), Key::I64(2), Key::I64(2)];
    glue.storage.delete_data("rarray", keys).unwrap();

    let actual = glue.execute("SELECT id FROM rarray").unwrap();
    let expected = vec![Payload::Select {
        labels: vec!["id".to_owned()],
        rows: vec![vec![Value::I64(1)]],
    }];
    assert_eq!(actual, expected);
}
