use {
    gluesql_core::{
        data::{Interval, Key, Schema, Value},
        store::{Store, StoreMut},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Connection, Error as SqliteError, ffi},
    tempfile::tempdir,
};

#[test]
fn primary_key_writes_preserve_rowids_without_decoding_existing_rows() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("primary.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    storage
        .insert_schema(
            &Schema::from_ddl("CREATE TABLE item (flag BOOLEAN, id INT PRIMARY KEY)").unwrap(),
        )
        .unwrap();
    storage
        .append_data(
            "item",
            vec![
                vec![Value::Bool(false), Value::I64(10)],
                vec![Value::Bool(false), Value::I64(20)],
            ],
        )
        .unwrap();

    // Writing other keys must not decode this invalid BOOLEAN.
    let sqlite = Connection::open(path).unwrap();
    sqlite
        .execute("UPDATE item SET flag = 2 WHERE id = 20", [])
        .unwrap();

    // Update an existing key, add a new one, and write the existing key again.
    storage
        .insert_data(
            "item",
            vec![
                (Key::I64(10), vec![Value::Bool(false), Value::I64(10)]),
                (Key::I64(30), vec![Value::Bool(true), Value::I64(30)]),
                (Key::I64(10), vec![Value::Bool(true), Value::I64(10)]),
            ],
        )
        .unwrap();

    let actual = sqlite
        .prepare("SELECT rowid, id, flag FROM item ORDER BY rowid")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let expected = vec![(1, 10, 1), (2, 20, 2), (3, 30, 1)];
    assert_eq!(actual, expected);
}

#[test]
fn rowid_writes_use_the_unshadowed_alias() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("rowid.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    storage
        .insert_schema(
            &Schema::from_ddl("CREATE TABLE item (_rowid_ INT, rowid INT, flag BOOLEAN)").unwrap(),
        )
        .unwrap();
    storage
        .insert_data(
            "item",
            vec![
                (
                    Key::I64(-7),
                    vec![Value::I64(100), Value::I64(200), Value::Bool(false)],
                ),
                (
                    Key::I64(20),
                    vec![Value::I64(300), Value::I64(400), Value::Bool(false)],
                ),
            ],
        )
        .unwrap();

    // oid is the physical row identifier; _rowid_ and rowid are ordinary columns.
    let sqlite = Connection::open(path).unwrap();
    sqlite
        .execute("UPDATE item SET flag = 2 WHERE oid = -7", [])
        .unwrap();
    storage
        .insert_data(
            "item",
            vec![
                (
                    Key::I64(20),
                    vec![Value::I64(500), Value::I64(600), Value::Bool(true)],
                ),
                (
                    Key::I64(21),
                    vec![Value::I64(700), Value::I64(800), Value::Bool(false)],
                ),
            ],
        )
        .unwrap();

    let actual = sqlite
        .prepare("SELECT oid, _rowid_, rowid, flag FROM item ORDER BY oid")
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
    let expected = vec![(-7, 100, 200, 2), (20, 500, 600, 1), (21, 700, 800, 0)];
    assert_eq!(actual, expected);
}

#[test]
fn primary_key_upserts_bind_encoded_values_and_quote_identifiers() {
    // Exercise TEXT, BLOB, RON, and REAL keys at SQLite's conflict boundary.
    let cases = [
        (Value::I128(i128::MIN), Value::I128(i128::MIN)),
        (
            Value::Str("한글\0' OR 1 = 1 --".to_owned()),
            Value::Str("한글\0' OR 1 = 1 --".to_owned()),
        ),
        (
            Value::Bytea(vec![0, 128, 255]),
            Value::Bytea(vec![0, 128, 255]),
        ),
        (
            Value::Interval(Interval::Month(i32::MIN)),
            Value::Interval(Interval::Month(i32::MIN)),
        ),
        (Value::F32(0.0), Value::F32(-0.0)),
        (Value::F64(0.0), Value::F64(-0.0)),
        (Value::F64(f64::INFINITY), Value::F64(f64::INFINITY)),
    ];

    for (initial, replacement) in cases {
        let directory = tempdir().unwrap();
        let path = directory.path().join("encoded.sqlite");
        let data_type = initial.get_type().unwrap();
        let mut schema = Schema::from_ddl(&format!(
            "CREATE TABLE item (id {data_type} PRIMARY KEY, marker TEXT)"
        ))
        .unwrap();
        schema.table_name = "key\"table".to_owned();
        let columns = schema.column_defs.as_mut().unwrap();
        columns[0].name = "key\"column".to_owned();
        columns[1].name = "value\"column".to_owned();

        let mut storage = SqliteStorage::new(&path).unwrap();
        storage.insert_schema(&schema).unwrap();
        storage
            .insert_data(
                "key\"table",
                vec![
                    (
                        Key::try_from(&initial).unwrap(),
                        vec![initial, Value::Str("first".to_owned())],
                    ),
                    (
                        Key::try_from(&replacement).unwrap(),
                        vec![replacement.clone(), Value::Str("second".to_owned())],
                    ),
                ],
            )
            .unwrap();

        let actual = storage
            .fetch_data("key\"table", &Key::try_from(&replacement).unwrap())
            .unwrap();
        let expected = Some(vec![replacement, Value::Str("second".to_owned())]);
        assert_eq!(actual, expected, "{data_type}");

        // Updating the conflicting key must retain the original physical row.
        let actual = Connection::open(path)
            .unwrap()
            .prepare(r#"SELECT rowid, "value""column" FROM "key""table""#)
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let expected = vec![(1, "second".to_owned())];
        assert_eq!(actual, expected, "{data_type}");
    }
}

#[test]
fn decimal_writes_reuse_rowids_across_scales_within_a_batch() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("decimal.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    storage
        .insert_schema(
            &Schema::from_ddl("CREATE TABLE item (id DECIMAL PRIMARY KEY, flag BOOLEAN)").unwrap(),
        )
        .unwrap();
    storage
        .append_data(
            "item",
            vec![
                vec![Value::Decimal("1.00".parse().unwrap()), Value::Bool(true)],
                vec![Value::Decimal("2.0".parse().unwrap()), Value::Bool(false)],
            ],
        )
        .unwrap();

    // Resolving logical decimal keys must not decode the other columns.
    let sqlite = Connection::open(path).unwrap();
    sqlite
        .execute("UPDATE item SET flag = 2 WHERE id = '2.0'", [])
        .unwrap();
    storage.insert_data("item", vec![]).unwrap();

    // Both an existing key and a key created in this batch change their TEXT scale.
    storage
        .insert_data(
            "item",
            vec![
                (
                    Key::Decimal("1.0".parse().unwrap()),
                    vec![Value::Decimal("1.000".parse().unwrap()), Value::Bool(true)],
                ),
                (
                    Key::Decimal("3".parse().unwrap()),
                    vec![Value::Decimal("3.00".parse().unwrap()), Value::Bool(false)],
                ),
                (
                    Key::Decimal("1.00".parse().unwrap()),
                    vec![Value::Decimal("1.0000".parse().unwrap()), Value::Bool(true)],
                ),
                (
                    Key::Decimal("3.0".parse().unwrap()),
                    vec![Value::Decimal("3.000".parse().unwrap()), Value::Bool(true)],
                ),
                (
                    Key::Decimal("1".parse().unwrap()),
                    vec![Value::Decimal("1.0".parse().unwrap()), Value::Bool(false)],
                ),
            ],
        )
        .unwrap();

    let actual = sqlite
        .prepare("SELECT rowid, id, flag FROM item ORDER BY rowid")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let expected = vec![
        (1, "1.0".to_owned(), 0),
        (2, "2.0".to_owned(), 2),
        (3, "3.000".to_owned(), 1),
    ];
    assert_eq!(actual, expected);
}

#[test]
fn other_unique_conflicts_roll_back_the_write_batch() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("unique.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    storage
        .insert_schema(
            &Schema::from_ddl("CREATE TABLE item (id INT PRIMARY KEY, name TEXT UNIQUE)").unwrap(),
        )
        .unwrap();
    storage
        .append_data(
            "item",
            vec![
                vec![Value::I64(10), Value::Str("first".to_owned())],
                vec![Value::I64(20), Value::Str("second".to_owned())],
            ],
        )
        .unwrap();

    // A new key must not replace a different row that has the same UNIQUE name.
    let actual = storage.insert_data(
        "item",
        vec![
            (
                Key::I64(10),
                vec![Value::I64(10), Value::Str("changed".to_owned())],
            ),
            (
                Key::I64(30),
                vec![Value::I64(30), Value::Str("second".to_owned())],
            ),
        ],
    );
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_UNIQUE),
        Some("UNIQUE constraint failed: item.name".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);

    let sqlite = Connection::open(path).unwrap();
    let mut read_rows = sqlite
        .prepare("SELECT rowid, id, name FROM item ORDER BY rowid")
        .unwrap();
    let actual = read_rows
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let expected_rows = vec![(1, 10, "first".to_owned()), (2, 20, "second".to_owned())];
    assert_eq!(actual, expected_rows);

    // A conflict while updating the PK must also undo earlier inserts in the batch.
    let actual = storage.insert_data(
        "item",
        vec![
            (
                Key::I64(30),
                vec![Value::I64(30), Value::Str("third".to_owned())],
            ),
            (
                Key::I64(10),
                vec![Value::I64(10), Value::Str("second".to_owned())],
            ),
        ],
    );
    assert_eq!(actual, expected);

    let actual = read_rows
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(actual, expected_rows);
}
