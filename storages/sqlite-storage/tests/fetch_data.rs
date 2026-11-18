use {
    gluesql_core::{
        data::{Interval, Key, Schema, Value},
        prelude::{Glue, Payload},
        store::{Store, StoreMut},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::Connection,
    tempfile::tempdir,
};

#[test]
fn primary_key_lookup_decodes_only_the_requested_row() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("primary.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "CREATE TABLE item (flag BOOLEAN, id INT PRIMARY KEY);
         INSERT INTO item VALUES (TRUE, 1), (FALSE, 2)",
    )
    .unwrap();

    // The first row cannot decode as BOOLEAN. Looking up the second must not read it.
    let sqlite = Connection::open(&path).unwrap();
    sqlite
        .execute("UPDATE item SET flag = 2 WHERE id = 1", [])
        .unwrap();

    assert_eq!(
        glue.execute("SELECT id, flag FROM item WHERE id = 2"),
        Ok(vec![Payload::Select {
            labels: vec!["id".to_owned(), "flag".to_owned()],
            rows: vec![vec![Value::I64(2), Value::Bool(false)]],
        }])
    );
    assert_eq!(
        glue.storage.fetch_data("item", &Key::I64(99)).unwrap(),
        None
    );

    // The same invalid value must still fail when its own row is requested.
    assert_eq!(
        glue.storage.fetch_data("item", &Key::I64(1)),
        Err(SqliteStorageError::InvalidBoolean(2).into())
    );
}

#[test]
fn decimal_key_lookup_decodes_only_the_matching_row() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("decimal.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "
        CREATE TABLE item (id DECIMAL PRIMARY KEY, flag BOOLEAN);
        INSERT INTO item VALUES
            (CAST('1.00' AS DECIMAL), FALSE),
            (CAST('2.00' AS DECIMAL), TRUE);
        ",
    )
    .unwrap();

    // A full scan would try to decode the first row's invalid BOOLEAN.
    let sqlite = Connection::open(path).unwrap();
    sqlite
        .execute("UPDATE item SET flag = 2 WHERE flag = 0", [])
        .unwrap();

    let key = Key::Decimal("2.0".parse().unwrap());
    let actual = glue.storage.fetch_data("item", &key);
    let expected = Ok(Some(vec![
        Value::Decimal("2.00".parse().unwrap()),
        Value::Bool(true),
    ]));
    assert_eq!(actual, expected);
}

#[test]
fn rowid_lookup_uses_the_unshadowed_alias() {
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
                    vec![Value::I64(700), Value::I64(800), Value::Bool(true)],
                ),
            ],
        )
        .unwrap();

    // Both preferred rowid aliases are user columns; oid identifies the stored row.
    let sqlite = Connection::open(&path).unwrap();
    sqlite
        .execute("UPDATE item SET flag = 2 WHERE oid = -7", [])
        .unwrap();

    assert_eq!(
        storage.fetch_data("item", &Key::I64(20)).unwrap(),
        Some(vec![Value::I64(700), Value::I64(800), Value::Bool(true)])
    );
    assert_eq!(storage.fetch_data("item", &Key::I64(21)).unwrap(), None);
    assert_eq!(storage.fetch_data("item", &Key::I32(20)).unwrap(), None);
}

#[test]
fn primary_key_bindings_support_physical_encodings_and_quoted_names() {
    // Cover native scalars, extended TEXT encodings, and typed RON as lookup parameters.
    let keys = [
        Value::I64(i64::MIN),
        Value::Bool(false),
        Value::F32(0.1),
        Value::F64(0.1),
        Value::Str("한글\0' OR 1 = 1 --".to_owned()),
        Value::Bytea(vec![0, 128, 255]),
        Value::I128(i128::MIN),
        Value::U64(u64::MAX),
        Value::Timestamp("2026-11-12T12:34:56.123456789".parse().unwrap()),
        Value::Interval(Interval::Microsecond(i64::MIN)),
    ];

    for value in keys {
        let data_type = value.get_type().unwrap();
        let mut schema =
            Schema::from_ddl(&format!("CREATE TABLE item (id {data_type} PRIMARY KEY)")).unwrap();
        schema.table_name = "key\"table".to_owned();
        schema.column_defs.as_mut().unwrap()[0].name = "key\"column".to_owned();

        let mut storage = SqliteStorage::in_memory().unwrap();
        storage.insert_schema(&schema).unwrap();
        storage
            .append_data("key\"table", vec![vec![value.clone()]])
            .unwrap();

        let key = Key::try_from(&value).unwrap();
        assert_eq!(
            storage.fetch_data("key\"table", &key).unwrap(),
            Some(vec![value]),
            "lookup binding for {data_type}"
        );
    }
}

#[test]
fn lookup_does_not_apply_sqlite_type_coercions_to_keys() {
    let mut storage = SqliteStorage::in_memory().unwrap();
    storage
        .insert_schema(&Schema::from_ddl("CREATE TABLE item (id INT PRIMARY KEY)").unwrap())
        .unwrap();
    storage
        .append_data("item", vec![vec![Value::I64(1)]])
        .unwrap();

    // These are distinct GlueSQL keys even when SQLite would compare their values equally.
    let different_keys = [
        Key::I32(1),
        Key::U64(1),
        Key::Bool(true),
        Key::Str("1".to_owned()),
        Key::try_from(Value::F64(1.0)).unwrap(),
        Key::None,
    ];
    for key in different_keys {
        assert_eq!(storage.fetch_data("item", &key).unwrap(), None, "{key:?}");
    }
}

#[test]
fn floating_keys_preserve_zero_equality_infinity_and_nan_misses() {
    let cases = [
        (
            Value::F32(0.0),
            Value::F32(-0.0),
            Value::F32(f32::INFINITY),
            Value::F32(f32::NAN),
        ),
        (
            Value::F64(0.0),
            Value::F64(-0.0),
            Value::F64(f64::INFINITY),
            Value::F64(f64::NAN),
        ),
    ];

    for (zero, negative_zero, infinity, nan) in cases {
        let data_type = zero.get_type().unwrap();
        let schema =
            Schema::from_ddl(&format!("CREATE TABLE item (id {data_type} PRIMARY KEY)")).unwrap();
        let mut storage = SqliteStorage::in_memory().unwrap();
        storage.insert_schema(&schema).unwrap();
        storage
            .append_data("item", vec![vec![zero.clone()], vec![infinity.clone()]])
            .unwrap();

        assert_eq!(
            storage
                .fetch_data("item", &Key::try_from(negative_zero).unwrap())
                .unwrap(),
            Some(vec![zero])
        );
        assert_eq!(
            storage
                .fetch_data("item", &Key::try_from(&infinity).unwrap())
                .unwrap(),
            Some(vec![infinity])
        );
        assert_eq!(
            storage
                .fetch_data("item", &Key::try_from(nan).unwrap())
                .unwrap(),
            None
        );
    }
}
