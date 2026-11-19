use {
    gluesql_core::{
        ast::DataType,
        data::{Key, Schema, Value},
        error::Result,
        prelude::Glue,
        store::{NativeQuery, ScanColumns, Store, StoreMut},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Error as SqliteError, ffi},
    tempfile::tempdir,
};

#[test]
fn decimal_keys_keep_numeric_equality_across_text_scales() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("decimal.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let schema = Schema::from_ddl("CREATE TABLE item (id DECIMAL PRIMARY KEY, name TEXT)").unwrap();
    storage.insert_schema(&schema).unwrap();

    // Different text scales describe the same logical primary key.
    storage
        .insert_data(
            "item",
            vec![(
                Key::Decimal("1.0".parse().unwrap()),
                vec![
                    Value::Decimal("1.00".parse().unwrap()),
                    Value::Str("first".to_owned()),
                ],
            )],
        )
        .unwrap();
    drop(storage);
    let mut storage = SqliteStorage::new(path).unwrap();
    assert_eq!(
        storage
            .fetch_data("item", &Key::Decimal("1".parse().unwrap()))
            .unwrap(),
        Some(vec![
            Value::Decimal("1.00".parse().unwrap()),
            Value::Str("first".to_owned())
        ])
    );

    // Rewriting that key must update the existing row rather than create a second one.
    storage
        .insert_data(
            "item",
            vec![(
                Key::Decimal("1.000".parse().unwrap()),
                vec![
                    Value::Decimal("1.0".parse().unwrap()),
                    Value::Str("updated".to_owned()),
                ],
            )],
        )
        .unwrap();
    let rows = storage
        .scan_data("item", ScanColumns::All)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![(
            Key::Decimal("1".parse().unwrap()),
            vec![
                Value::Decimal("1.0".parse().unwrap()),
                Value::Str("updated".to_owned())
            ]
        )]
    );

    storage
        .delete_data("item", vec![Key::Decimal("1.00".parse().unwrap())])
        .unwrap();
    assert_eq!(
        storage
            .fetch_data("item", &Key::Decimal("1".parse().unwrap()))
            .unwrap(),
        None
    );
}

#[test]
fn decimal_unique_values_use_numeric_equality_and_order() {
    let mut storage = SqliteStorage::in_memory().unwrap();
    let schema = Schema::from_ddl("CREATE TABLE prices (amount DECIMAL UNIQUE)").unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .append_data(
            "prices",
            vec![
                vec![Value::Decimal("10.0".parse().unwrap())],
                vec![Value::Decimal("-2.00".parse().unwrap())],
                vec![Value::Decimal("2.000".parse().unwrap())],
            ],
        )
        .unwrap();

    let actual = storage.append_data("prices", vec![vec![Value::Decimal("2.0".parse().unwrap())]]);
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_UNIQUE),
        Some("UNIQUE constraint failed: prices.amount".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);

    // Run ORDER BY in SQLite and read TEXT to check both numeric order and stored scale.
    let actual = storage
        .query_native(
            "SELECT amount FROM prices ORDER BY amount",
            &[],
            &[DataType::Text],
        )
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let expected = vec![
        vec![Value::Str("-2.00".to_owned())],
        vec![Value::Str("2.000".to_owned())],
        vec![Value::Str("10.0".to_owned())],
    ];
    assert_eq!(actual, expected);
}

#[test]
fn decimal_foreign_keys_keep_scales_after_reopen() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("decimal.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "
        CREATE TABLE parent (id DECIMAL PRIMARY KEY);
        CREATE TABLE child (
            id INT PRIMARY KEY,
            parent_id DECIMAL,
            FOREIGN KEY (parent_id) REFERENCES parent (id)
        );
        INSERT INTO parent VALUES (CAST('1.00' AS DECIMAL));
        INSERT INTO child VALUES (10, CAST('1.0' AS DECIMAL));
        ",
    )
    .unwrap();

    glue.execute("UPDATE child SET parent_id = CAST('1.000' AS DECIMAL) WHERE id = 10")
        .unwrap();

    // A keyed write can replace the parent's scale while the child still references it.
    glue.storage
        .insert_data(
            "parent",
            vec![(
                Key::Decimal("1".parse().unwrap()),
                vec![Value::Decimal("1.0000".parse().unwrap())],
            )],
        )
        .unwrap();
    drop(glue);

    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute("INSERT INTO child VALUES (20, CAST('1.00000' AS DECIMAL))")
        .unwrap();

    // Bypass the common executor's FK check to verify SQLite still rejects missing parents.
    let actual = glue.storage.append_data(
        "child",
        vec![vec![Value::I64(30), Value::Decimal("2.0".parse().unwrap())]],
    );
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_FOREIGNKEY),
        Some("FOREIGN KEY constraint failed".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);

    let actual = glue
        .storage
        .query_native(
            "SELECT rowid, id FROM parent",
            &[],
            &[DataType::Int, DataType::Text],
        )
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let expected = vec![vec![Value::I64(1), Value::Str("1.0000".to_owned())]];
    assert_eq!(actual, expected);

    let actual = glue
        .storage
        .query_native(
            "SELECT id, parent_id FROM child ORDER BY id",
            &[],
            &[DataType::Int, DataType::Text],
        )
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let expected = vec![
        vec![Value::I64(10), Value::Str("1.000".to_owned())],
        vec![Value::I64(20), Value::Str("1.00000".to_owned())],
    ];
    assert_eq!(actual, expected);
}
