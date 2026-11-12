use {
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Error,
        prelude::Glue,
        store::{Store, StoreMut, Transaction},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Connection, Error as SqliteError, ffi},
    tempfile::tempdir,
};

#[test]
fn failed_batches_are_atomic_and_preserve_earlier_explicit_writes() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("batch.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    storage
        .insert_schema(&Schema::from_ddl("CREATE TABLE item (id INT PRIMARY KEY)").unwrap())
        .unwrap();

    // A failed append must roll back the earlier row in the same batch.
    let actual = storage.append_data("item", vec![vec![Value::I64(1)], vec![]]);
    let expected = Err(SqliteStorageError::ConflictRowLength {
        expected: 1,
        actual: 0,
    }
    .into());
    assert_eq!(actual, expected);
    assert_eq!(storage.fetch_data("item", &Key::I64(1)).unwrap(), None);

    // A failed insert batch must preserve writes made before that batch.
    storage.begin(false).unwrap();
    storage
        .append_data("item", vec![vec![Value::I64(2)]])
        .unwrap();
    let actual = storage.insert_data(
        "item",
        vec![(Key::I64(2), vec![Value::I64(2)]), (Key::I64(3), vec![])],
    );
    assert_eq!(actual, expected);
    assert_eq!(
        storage.fetch_data("item", &Key::I64(2)).unwrap(),
        Some(vec![Value::I64(2)])
    );
    assert_eq!(storage.fetch_data("item", &Key::I64(3)).unwrap(), None);

    storage.commit().unwrap();
    drop(storage);

    let storage = SqliteStorage::new(path).unwrap();
    assert_eq!(
        storage.fetch_data("item", &Key::I64(2)).unwrap(),
        Some(vec![Value::I64(2)])
    );
}

#[test]
fn failed_commits_clean_up_automatic_writes_and_allow_explicit_retry() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("busy.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "CREATE TABLE item (id INT PRIMARY KEY);
         INSERT INTO item VALUES (0)",
    )
    .unwrap();

    // A separate reader holds the lock that prevents the writer from committing.
    let reader = Connection::open(&path).unwrap();
    reader
        .execute_batch(
            "BEGIN;
             SELECT * FROM item",
        )
        .unwrap();

    let busy = Error::from(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_BUSY),
        Some("database is locked".to_owned()),
    )));

    // Releasing the outermost savepoint can fail when Store is used directly.
    let actual = glue.storage.append_data("item", vec![vec![Value::I64(1)]]);
    assert_eq!(actual.unwrap_err(), busy);
    assert_eq!(glue.storage.fetch_data("item", &Key::I64(1)).unwrap(), None);

    // Glue's automatic COMMIT has the same cleanup requirement.
    let actual = glue.execute("INSERT INTO item VALUES (2)");
    assert_eq!(actual.unwrap_err(), busy);
    assert_eq!(glue.storage.fetch_data("item", &Key::I64(2)).unwrap(), None);

    // An explicit transaction keeps its pending write so COMMIT can be retried.
    glue.storage.begin(false).unwrap();
    glue.storage
        .append_data("item", vec![vec![Value::I64(3)]])
        .unwrap();
    let actual = glue.storage.commit();
    assert_eq!(actual.unwrap_err(), busy);
    assert_eq!(
        glue.storage.fetch_data("item", &Key::I64(3)).unwrap(),
        Some(vec![Value::I64(3)])
    );

    reader.execute_batch("ROLLBACK").unwrap();
    glue.storage.commit().unwrap();
    drop(glue);

    let storage = SqliteStorage::new(path).unwrap();
    assert_eq!(
        storage.fetch_data("item", &Key::I64(3)).unwrap(),
        Some(vec![Value::I64(3)])
    );
    assert_eq!(storage.fetch_data("item", &Key::I64(1)).unwrap(), None);
    assert_eq!(storage.fetch_data("item", &Key::I64(2)).unwrap(), None);
}

#[test]
fn sqlite_transaction_abort_preserves_the_original_error() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("abort.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute("CREATE TABLE item (id INT)").unwrap();

    // SQLite rolls back the entire transaction before GlueSQL can perform its cleanup.
    let sqlite = Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            "CREATE TRIGGER abort_insert
             BEFORE INSERT ON item
             BEGIN
                 SELECT RAISE(ROLLBACK, 'abort batch');
             END",
        )
        .unwrap();

    let expected = Error::from(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_TRIGGER),
        Some("abort batch".to_owned()),
    )));
    assert_eq!(glue.execute("INSERT INTO item VALUES (1)"), Err(expected));

    // Cleanup must leave the connection ready for a new transaction.
    assert!(glue.storage.begin(false).unwrap());
    glue.storage.rollback().unwrap();

    sqlite.execute_batch("DROP TRIGGER abort_insert").unwrap();
    glue.execute("INSERT INTO item VALUES (2)").unwrap();
    assert_eq!(
        glue.storage.fetch_data("item", &Key::I64(1)).unwrap(),
        Some(vec![Value::I64(2)])
    );
}

#[test]
fn explicit_transactions_cannot_nest_or_end_twice() {
    let mut storage = SqliteStorage::in_memory().unwrap();

    // An automatic begin joins an explicit transaction; another explicit begin cannot.
    assert!(storage.begin(false).unwrap());
    assert!(!storage.begin(true).unwrap());
    assert_eq!(
        storage.begin(false),
        Err(SqliteStorageError::NestedTransaction.into())
    );

    // Once rolled back, there is no transaction left for either ending operation.
    storage.rollback().unwrap();
    let expected = Err(SqliteStorageError::TransactionNotFound.into());
    assert_eq!(storage.commit(), expected);
    assert_eq!(storage.rollback(), expected);
}

#[test]
fn schema_and_metadata_roll_back_together_on_disk() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("catalog.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());

    // Rolling back the first CREATE must also remove newly created metadata tables.
    glue.execute(
        "BEGIN;
         CREATE TABLE item (id INT);
         ROLLBACK",
    )
    .unwrap();

    let sqlite = Connection::open(&path).unwrap();
    let count: i64 = sqlite
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);

    // Rolling back DROP must restore both the table and its comment metadata.
    glue.execute(
        "CREATE TABLE item (id INT COMMENT 'id');
         BEGIN;
         DROP TABLE item;
         ROLLBACK",
    )
    .unwrap();
    let schema = glue.storage.fetch_schema("item").unwrap().unwrap();
    let columns = schema.column_defs.unwrap();
    assert_eq!(columns[0].comment, Some("id".to_owned()));

    // A committed DROP removes the table's metadata rows from both metadata tables.
    glue.execute("DROP TABLE item").unwrap();
    let count: i64 = sqlite
        .query_row(
            "SELECT
                 (SELECT count(*) FROM __gluesql_table_meta) +
                 (SELECT count(*) FROM __gluesql_column_meta)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(glue.storage.fetch_all_schemas().unwrap(), vec![]);
}
