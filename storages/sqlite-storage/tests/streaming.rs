use {
    gluesql_core::{
        data::{Key, Schema, Value},
        error::{Error, Result},
        prelude::Glue,
        store::{ScanColumns, Store, StoreMut},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Connection, Error as SqliteError, ffi},
    std::time::Duration,
    tempfile::tempdir,
};

#[test]
fn rowid_scan_streams_projected_rows_and_releases_the_cursor_on_drop() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("lazy.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    storage
        .insert_schema(&Schema::from_ddl("CREATE TABLE item (flag BOOLEAN)").unwrap())
        .unwrap();
    storage
        .insert_data(
            "item",
            vec![
                (Key::I64(8), vec![Value::Bool(false)]),
                (Key::I64(-2), vec![Value::Bool(true)]),
                (Key::I64(3), vec![Value::Bool(true)]),
            ],
        )
        .unwrap();

    let sqlite = Connection::open(&path).unwrap();
    sqlite.busy_timeout(Duration::ZERO).unwrap();

    // The middle row in rowid order has an INTEGER that cannot decode as BOOLEAN.
    sqlite
        .execute("UPDATE item SET flag = 2 WHERE rowid = 3", [])
        .unwrap();

    // The projection may be dropped before iteration, and the bad later row stays unread.
    let mut rows = {
        let projection = [("flag".to_owned(), 0)];
        storage
            .scan_data("item", ScanColumns::Selected(&projection))
            .unwrap()
    };
    assert_eq!(
        rows.next(),
        Some(Ok((Key::I64(-2), vec![Value::Bool(true)])))
    );

    // Another live statement can scan the same connection, including an empty projection.
    let keys_only = storage
        .scan_data("item", ScanColumns::Selected(&[]))
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        keys_only,
        vec![
            (Key::I64(-2), vec![]),
            (Key::I64(3), vec![]),
            (Key::I64(8), vec![])
        ],
    );

    // Dropping the unfinished scan must release the read lock for another connection.
    drop(rows);
    let updated = sqlite
        .execute("UPDATE item SET flag = 0 WHERE rowid = 3", [])
        .unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn rowid_scan_releases_the_statement_after_error_and_exhaustion() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("completion.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    storage
        .insert_schema(&Schema::from_ddl("CREATE TABLE item (flag BOOLEAN)").unwrap())
        .unwrap();
    storage
        .append_data(
            "item",
            vec![vec![Value::Bool(true)], vec![Value::Bool(false)]],
        )
        .unwrap();

    let sqlite = Connection::open(&path).unwrap();
    sqlite.busy_timeout(Duration::ZERO).unwrap();
    sqlite
        .execute("UPDATE item SET flag = 2 WHERE rowid = 2", [])
        .unwrap();

    let mut rows = storage.scan_data("item", ScanColumns::All).unwrap();
    assert_eq!(
        rows.next(),
        Some(Ok((Key::I64(1), vec![Value::Bool(true)])))
    );
    assert_eq!(
        rows.next(),
        Some(Err(SqliteStorageError::InvalidBoolean(2).into()))
    );
    assert_eq!(rows.next(), None);

    // The returned iterator still exists, but its failed statement no longer holds a read lock.
    let updated = sqlite
        .execute("UPDATE item SET flag = 0 WHERE rowid = 2", [])
        .unwrap();
    assert_eq!(updated, 1);
    assert_eq!(rows.next(), None);

    // Reaching EOF also releases the statement while the iterator is still in scope.
    let mut complete = storage.scan_data("item", ScanColumns::All).unwrap();
    assert_eq!(
        complete.next(),
        Some(Ok((Key::I64(1), vec![Value::Bool(true)])))
    );
    assert_eq!(
        complete.next(),
        Some(Ok((Key::I64(2), vec![Value::Bool(false)])))
    );
    assert_eq!(complete.next(), None);

    sqlite.execute_batch("DROP TABLE item").unwrap();
    assert_eq!(complete.next(), None);
}

#[test]
fn a_sqlite_step_error_ends_the_scan() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("busy.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    storage
        .insert_schema(&Schema::from_ddl("CREATE TABLE item (id INT)").unwrap())
        .unwrap();
    storage
        .append_data("item", vec![vec![Value::I64(7)]])
        .unwrap();

    let mut rows = storage.scan_data("item", ScanColumns::All).unwrap();

    // Lock after preparing the scan so SQLite fails on the first step, before decoding.
    let sqlite = Connection::open(&path).unwrap();
    sqlite.busy_timeout(Duration::ZERO).unwrap();
    sqlite.execute_batch("BEGIN EXCLUSIVE").unwrap();

    let expected = Error::from(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_BUSY),
        Some("database is locked".to_owned()),
    )));
    assert_eq!(rows.next(), Some(Err(expected)));

    // Unlocking permits a new scan, but must not revive the failed iterator.
    sqlite.execute_batch("ROLLBACK").unwrap();
    assert_eq!(rows.next(), None);
    let actual = storage
        .scan_data("item", ScanColumns::All)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(actual, vec![(Key::I64(1), vec![Value::I64(7)])]);
}

#[test]
fn integer_primary_key_scan_decodes_rows_as_consumed() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("primary-key.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "CREATE TABLE item (flag BOOLEAN, id INT PRIMARY KEY);
         INSERT INTO item VALUES (FALSE, 10), (TRUE, 2);",
    )
    .unwrap();

    Connection::open(&path)
        .unwrap()
        .execute("UPDATE item SET flag = 2 WHERE id = 10", [])
        .unwrap();

    let projection = [("flag".to_owned(), 0)];
    let mut rows = glue
        .storage
        .scan_data("item", ScanColumns::Selected(&projection))
        .unwrap();
    assert_eq!(
        rows.next(),
        Some(Ok((Key::I64(2), vec![Value::Bool(true)])))
    );
    assert_eq!(
        rows.next(),
        Some(Err(SqliteStorageError::InvalidBoolean(2).into()))
    );
}

#[test]
fn decimal_primary_key_scan_uses_numeric_order() {
    let mut storage = SqliteStorage::in_memory().unwrap();
    storage
        .insert_schema(&Schema::from_ddl("CREATE TABLE item (id DECIMAL PRIMARY KEY)").unwrap())
        .unwrap();

    // The DECIMAL collation puts "2" before "10" even though the key is stored as TEXT.
    storage
        .append_data(
            "item",
            vec![
                vec![Value::Decimal("2".parse().unwrap())],
                vec![Value::Decimal("10".parse().unwrap())],
                vec![Value::Decimal("-1".parse().unwrap())],
            ],
        )
        .unwrap();

    let actual = storage
        .scan_data("item", ScanColumns::All)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let expected = vec![
        (
            Key::Decimal("-1".parse().unwrap()),
            vec![Value::Decimal("-1".parse().unwrap())],
        ),
        (
            Key::Decimal("2".parse().unwrap()),
            vec![Value::Decimal("2".parse().unwrap())],
        ),
        (
            Key::Decimal("10".parse().unwrap()),
            vec![Value::Decimal("10".parse().unwrap())],
        ),
    ];
    assert_eq!(actual, expected);
}

#[test]
fn int128_primary_key_scan_sorts_text_encoded_keys_as_integers() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "CREATE TABLE item (id INT128 PRIMARY KEY);
         INSERT INTO item VALUES (10), (2);",
    )
    .unwrap();

    let actual = glue
        .storage
        .scan_data("item", ScanColumns::Selected(&[]))
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let expected = vec![(Key::I128(2), vec![]), (Key::I128(10), vec![])];
    assert_eq!(actual, expected);
}

#[test]
fn text_primary_key_scan_preserves_string_order_in_utf16_databases() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("utf16.sqlite");

    // SQLite records the file encoding when the first table is created.
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "PRAGMA encoding = 'UTF-16le';
             CREATE TABLE seed (id);
             DROP TABLE seed;",
        )
        .unwrap();

    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "CREATE TABLE item (id TEXT PRIMARY KEY);
         INSERT INTO item VALUES ('Ā'), ('ÿ');",
    )
    .unwrap();

    // UTF-16le stores Ā as 00 01 and ÿ as FF 00, reversing their Rust string order.
    let actual = glue
        .storage
        .scan_data("item", ScanColumns::Selected(&[]))
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let expected = vec![
        (Key::Str("ÿ".to_owned()), vec![]),
        (Key::Str("Ā".to_owned()), vec![]),
    ];
    assert_eq!(actual, expected);
}
