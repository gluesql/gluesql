use {
    super::column,
    gluesql_core::{
        ast::DataType,
        data::{Key, Schema, Value},
        prelude::{Glue, Payload},
        store::{AlterTable, NativeQuery, Store, StoreMut, Transaction},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Connection, Error as SqliteError, ffi},
    rust_decimal::Decimal,
    tempfile::tempdir,
};

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
    sqlite
        .create_collation("gluesql_decimal", |left, right| {
            let left: Decimal = left.parse().unwrap();
            let right: Decimal = right.parse().unwrap();

            left.cmp(&right)
        })
        .unwrap();
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
fn dropping_primary_key_preserves_rows_and_later_writes() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());

    // The logical primary keys differ from the SQLite rowids used after the drop.
    glue.execute(
        "
        CREATE TABLE item (id INT PRIMARY KEY, name TEXT);
        INSERT INTO item VALUES (10, 'a'), (20, 'b');
        ",
    )
    .unwrap();

    let actual = glue.execute("ALTER TABLE item DROP COLUMN id").unwrap();
    let expected = vec![Payload::AlterTable];
    assert_eq!(actual, expected);

    let actual = glue.execute("SELECT name FROM item ORDER BY name").unwrap();
    let expected = vec![Payload::Select {
        labels: vec!["name".to_owned()],
        rows: vec![
            vec![Value::Str("a".to_owned())],
            vec![Value::Str("b".to_owned())],
        ],
    }];
    assert_eq!(actual, expected);

    let actual = glue
        .execute(
            "
            UPDATE item SET name = 'changed' WHERE name = 'a';
            DELETE FROM item WHERE name = 'b';
            INSERT INTO item VALUES ('new');
            ",
        )
        .unwrap();
    let expected = vec![Payload::Update(1), Payload::Delete(1), Payload::Insert(1)];
    assert_eq!(actual, expected);

    let actual = glue.execute("SELECT name FROM item ORDER BY name").unwrap();
    let expected = vec![Payload::Select {
        labels: vec!["name".to_owned()],
        rows: vec![
            vec![Value::Str("changed".to_owned())],
            vec![Value::Str("new".to_owned())],
        ],
    }];
    assert_eq!(actual, expected);
}

#[test]
fn failed_column_copy_preserves_references_and_earlier_transaction_writes() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("copy_failure.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let schema =
        Schema::from_ddl("CREATE TABLE item (id INT PRIMARY KEY) COMMENT = 'original'").unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .append_data("item", vec![vec![Value::I64(10)], vec![Value::I64(20)]])
        .unwrap();
    storage
        .insert_schema(
            &Schema::from_ddl(
                "CREATE TABLE child (
                     item_id INT,
                     CONSTRAINT item_fk FOREIGN KEY (item_id) REFERENCES item (id)
                 )",
            )
            .unwrap(),
        )
        .unwrap();
    storage
        .append_data("child", vec![vec![Value::I64(10)]])
        .unwrap();

    storage.begin(false).unwrap();
    storage
        .append_data("item", vec![vec![Value::I64(30)]])
        .unwrap();

    // The first copied row succeeds; the second violates the new UNIQUE constraint.
    let actual = storage.add_column("item", &column("code INT UNIQUE DEFAULT 1"), Value::I64(1));
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_UNIQUE),
        Some("UNIQUE constraint failed: item.code".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);

    // A savepoint rollback alone does not reset connection-level FK deferral.
    let actual = storage
        .query_native("PRAGMA defer_foreign_keys", &[], &[DataType::Int])
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected = vec![vec![Value::I64(0)]];
    assert_eq!(actual, expected);

    let actual = storage.append_data("child", vec![vec![Value::I64(99)]]);
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CONSTRAINT_FOREIGNKEY),
        Some("FOREIGN KEY constraint failed".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);

    storage.commit().unwrap();
    drop(storage);

    let storage = SqliteStorage::new(&path).unwrap();
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(schema));
    let actual = storage.fetch_data("child", &Key::I64(1)).unwrap();
    let expected = Some(vec![Value::I64(10)]);
    assert_eq!(actual, expected);

    let sqlite = Connection::open(path).unwrap();
    let actual = sqlite
        .prepare("SELECT rowid, id FROM item ORDER BY rowid")
        .unwrap()
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let expected = vec![(1, 10), (2, 20), (3, 30)];
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
