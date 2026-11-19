use {
    super::{expect_native_source, expect_query},
    gluesql_core::{ast::DataType, data::Value, prelude::Glue, store::NativeQuery},
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Connection, Error as SqliteError, ffi},
    std::time::Duration,
    tempfile::tempdir,
};

#[test]
fn native_rows_outlive_query_arguments() {
    let storage = SqliteStorage::in_memory().unwrap();
    let mut rows = {
        let sql = "SELECT ?1".to_owned();
        let params = vec![Value::Str("retained".to_owned())];
        let types = vec![DataType::Text];
        storage.query_native(&sql, &params, &types).unwrap()
    };

    // All three argument buffers have been dropped before the first row is read.
    let actual = rows.next();
    let expected = Some(Ok(vec![Value::Str("retained".to_owned())]));
    assert_eq!(actual, expected);

    let actual = rows.next();
    let expected = None;
    assert_eq!(actual, expected);
}

#[test]
fn native_api_validates_statements_before_iteration() {
    let storage = SqliteStorage::in_memory().unwrap();

    // NativeQuery is a read interface; reject mutation before stepping it.
    let actual = storage
        .query_native("CREATE TABLE forbidden (id INTEGER)", &[], &[])
        .err();
    let expected = Some(SqliteStorageError::NativeStatementNotReadOnly.into());
    assert_eq!(actual, expected);

    // Result shape is checked even when the query returns no rows.
    let actual = storage
        .query_native("SELECT 1 WHERE FALSE", &[], &[DataType::Int, DataType::Int])
        .err();
    let expected = Some(
        SqliteStorageError::NativeColumnCountMismatch {
            expected: 2,
            actual: 1,
        }
        .into(),
    );
    assert_eq!(actual, expected);

    // A missing parameter is a binding error, before any row is requested.
    let actual = storage
        .query_native("SELECT ?1", &[], &[DataType::Int])
        .err();
    let expected =
        Some(SqliteStorageError::Sqlite(SqliteError::InvalidParameterCount(0, 1)).into());
    assert_eq!(actual, expected);

    // A typed parameter still has to pass the SQLite value codec.
    let actual = storage
        .query_native("SELECT ?1", &[Value::F64(f64::NAN)], &[DataType::Float])
        .err();
    let expected = Some(SqliteStorageError::NanNotSupported.into());
    assert_eq!(actual, expected);
}

#[test]
fn exhausting_a_native_cursor_releases_its_read_lock() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("exhaustion.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "
        CREATE TABLE Item (flag BOOLEAN);
        INSERT INTO Item VALUES (TRUE), (FALSE);
        ",
    )
    .unwrap();
    let writer = Connection::open(&path).unwrap();
    writer.busy_timeout(Duration::ZERO).unwrap();

    let mut rows = glue
        .storage
        .query_native(
            "SELECT flag FROM Item ORDER BY rowid",
            &[],
            &[DataType::Boolean],
        )
        .unwrap();
    assert_eq!(rows.next(), Some(Ok(vec![Value::Bool(true)])));
    assert_eq!(rows.next(), Some(Ok(vec![Value::Bool(false)])));

    let actual = writer.execute("UPDATE Item SET flag = 1 WHERE rowid = 2", []);
    let expected = Err(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_BUSY),
        Some("database is locked".to_owned()),
    ));
    assert_eq!(actual, expected);

    let actual = rows.next();
    let expected = None;
    assert_eq!(actual, expected);

    // The iterator is still in scope, but reaching EOF has released its statement.
    let actual = writer
        .execute("UPDATE Item SET flag = 1 WHERE rowid = 2", [])
        .unwrap();
    let expected = 1;
    assert_eq!(actual, expected);

    let actual = rows.next();
    let expected = None;
    assert_eq!(actual, expected);
}

#[test]
fn dropping_a_native_cursor_releases_its_read_lock() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("drop.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "
        CREATE TABLE Item (flag BOOLEAN);
        INSERT INTO Item VALUES (TRUE), (FALSE);
        ",
    )
    .unwrap();
    let writer = Connection::open(&path).unwrap();
    writer.busy_timeout(Duration::ZERO).unwrap();

    let mut rows = glue
        .storage
        .query_native(
            "SELECT flag FROM Item ORDER BY rowid",
            &[],
            &[DataType::Boolean],
        )
        .unwrap();
    let actual = rows.next();
    let expected = Some(Ok(vec![Value::Bool(true)]));
    assert_eq!(actual, expected);

    let actual = writer.execute("UPDATE Item SET flag = 1 WHERE rowid = 2", []);
    let expected = Err(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_BUSY),
        Some("database is locked".to_owned()),
    ));
    assert_eq!(actual, expected);

    // Drop with the second row still unread, then write from the other connection.
    drop(rows);
    let actual = writer
        .execute("UPDATE Item SET flag = 1 WHERE rowid = 2", [])
        .unwrap();
    let expected = 1;
    assert_eq!(actual, expected);
}

#[test]
fn native_cursor_reports_late_decode_errors_and_releases_its_lock() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("decode-error.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "
        CREATE TABLE Item (flag BOOLEAN);
        INSERT INTO Item VALUES (TRUE), (FALSE);
        ",
    )
    .unwrap();

    let plan = glue
        .plan("SELECT flag FROM Item WHERE TRUE")
        .unwrap()
        .remove(0);
    expect_native_source(expect_query(&plan));

    // SQLite accepts 2 as INTEGER, but the second row cannot decode as GlueSQL BOOLEAN.
    let writer = Connection::open(&path).unwrap();
    writer.busy_timeout(Duration::ZERO).unwrap();
    writer
        .execute("UPDATE Item SET flag = 2 WHERE rowid = 2", [])
        .unwrap();

    let actual = glue.execute_prepared(&plan);
    let expected = Err(SqliteStorageError::InvalidBoolean(2).into());
    assert_eq!(actual, expected);

    // Direct iteration proves the valid first row is available before the later error.
    let mut rows = glue
        .storage
        .query_native(
            "SELECT flag FROM Item ORDER BY rowid",
            &[],
            &[DataType::Boolean],
        )
        .unwrap();
    let actual = rows.next();
    let expected = Some(Ok(vec![Value::Bool(true)]));
    assert_eq!(actual, expected);

    let actual = writer.execute("UPDATE Item SET flag = 0 WHERE rowid = 2", []);
    let expected = Err(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_BUSY),
        Some("database is locked".to_owned()),
    ));
    assert_eq!(actual, expected);

    let actual = rows.next();
    let expected = Some(Err(SqliteStorageError::InvalidBoolean(2).into()));
    assert_eq!(actual, expected);

    // The error itself must release the lock, without another next() or drop().
    let actual = writer
        .execute("UPDATE Item SET flag = 0 WHERE rowid = 2", [])
        .unwrap();
    let expected = 1;
    assert_eq!(actual, expected);

    // Repairing the stored row must not revive the failed iterator.
    let actual = rows.next();
    let expected = None;
    assert_eq!(actual, expected);
}

#[test]
fn a_sqlite_step_error_ends_the_native_cursor() {
    let storage = SqliteStorage::in_memory().unwrap();
    let mut rows = storage
        .query_native("SELECT abs(-9223372036854775808)", &[], &[DataType::Int])
        .unwrap();

    // Preparation succeeds; SQLite raises integer overflow when stepping the query.
    let actual = rows.next();
    let expected = Some(Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_ERROR),
        Some("integer overflow".to_owned()),
    ))
    .into()));
    assert_eq!(actual, expected);

    let actual = rows.next();
    let expected = None;
    assert_eq!(actual, expected);
}
