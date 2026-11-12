use {
    gluesql_core::{
        ast::DataType,
        data::{Key, Schema, Value},
        error::{Error, TranslateError},
        store::{ScanColumns, Store, StoreMut},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::Connection,
    tempfile::tempdir,
};

#[test]
fn internal_table_names_are_reserved_and_hidden() {
    let mut storage = SqliteStorage::in_memory().unwrap();

    // Internal names remain reserved even when the caller changes their case.
    let reserved = Schema::from_ddl("CREATE TABLE __GLUESQL_COLUMN_META (id INT)").unwrap();
    let expected = Err(SqliteStorageError::ReservedTableName(reserved.table_name.clone()).into());
    assert_eq!(storage.insert_schema(&reserved), expected);
    assert_eq!(storage.delete_schema(&reserved.table_name), expected);
    assert_eq!(storage.fetch_schema(&reserved.table_name).unwrap(), None);

    // A user table starting with "sqlite" is not an internal metadata table.
    let basic = Schema::from_ddl("CREATE TABLE item (id INT)").unwrap();
    let prefix = Schema::from_ddl("CREATE TABLE sqliteUser (id INT)").unwrap();
    storage.insert_schema(&basic).unwrap();
    storage.insert_schema(&prefix).unwrap();
    assert_eq!(storage.fetch_all_schemas().unwrap(), vec![basic, prefix]);
}

#[test]
fn schema_guards_leave_the_database_unchanged() {
    let mut storage = SqliteStorage::in_memory().unwrap();
    let basic = Schema::from_ddl("CREATE TABLE item (id INT)").unwrap();

    // Index definitions cannot be supplied through insert_schema.
    let indexed =
        Schema::from_ddl("CREATE TABLE indexed (id INT); CREATE INDEX idx ON indexed (id)")
            .unwrap();
    assert_eq!(
        storage.insert_schema(&indexed),
        Err(SqliteStorageError::SchemaIndexesNotSupported.into())
    );
    assert_eq!(storage.fetch_all_schemas().unwrap(), vec![]);

    // An engine selection cannot be applied to this SQLite connection.
    let mut engine = basic.clone();
    engine.engine = Some("other".to_owned());
    assert_eq!(
        storage.insert_schema(&engine),
        Err(SqliteStorageError::EngineNotSupported("other".to_owned()).into())
    );
    assert_eq!(storage.fetch_all_schemas().unwrap(), vec![]);

    // Some([]) is an empty schema, not a schemaless document table (None).
    let mut empty = basic.clone();
    empty.column_defs = Some(vec![]);
    assert_eq!(
        storage.insert_schema(&empty),
        Err(SqliteStorageError::EmptySchema("item".to_owned()).into())
    );
    assert_eq!(storage.fetch_all_schemas().unwrap(), vec![]);

    // Shadowing all three aliases leaves no way to address SQLite's rowid.
    let hidden = Schema::from_ddl("CREATE TABLE hidden (_rowid_ INT, rowid INT, oid INT)").unwrap();
    assert_eq!(
        storage.insert_schema(&hidden),
        Err(SqliteStorageError::RowidUnavailable("hidden".to_owned()).into())
    );
    assert_eq!(storage.fetch_all_schemas().unwrap(), vec![]);

    // Missing schema lookup/deletion is harmless; row access requires a table.
    storage.delete_schema("absent").unwrap();
    assert_eq!(storage.fetch_schema("absent").unwrap(), None);
    assert_eq!(
        storage.fetch_data("absent", &Key::I64(1)),
        Err(SqliteStorageError::TableNotFound("absent".to_owned()).into())
    );

    // A failed switch to schemaless storage must keep the existing column schema.
    storage.insert_schema(&basic).unwrap();
    let schemaless = Schema {
        column_defs: None,
        ..basic.clone()
    };
    assert_eq!(
        storage.insert_schema(&schemaless),
        Err(SqliteStorageError::SchemaKindChangeNotSupported("item".to_owned()).into())
    );
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(basic));
}

#[test]
fn row_keys_follow_primary_keys_or_sqlite_rowid() {
    let mut storage = SqliteStorage::in_memory().unwrap();

    // With a primary key, the supplied Key must agree with the value in the row.
    let schema = Schema::from_ddl("CREATE TABLE item (id INT PRIMARY KEY, name TEXT)").unwrap();
    storage.insert_schema(&schema).unwrap();

    let actual = storage.insert_data(
        "item",
        vec![(Key::I64(1), vec![Value::I64(2), Value::Null])],
    );
    let expected = SqliteStorageError::ConflictPrimaryKey {
        expected: Key::I64(1),
        actual: Key::I64(2),
    };
    assert_eq!(actual, Err(expected.into()));

    // Without a primary key, shadowing _rowid_ and rowid makes Store use oid.
    // These user columns hold ordinary values; the separate Key must be an integer.
    let schema = Schema::from_ddl("CREATE TABLE rows (_rowid_ INT, rowid INT)").unwrap();
    storage.insert_schema(&schema).unwrap();
    let actual = storage.insert_data(
        "rows",
        vec![(
            Key::Str("key".to_owned()),
            vec![Value::I64(1), Value::I64(2)],
        )],
    );
    let expected = SqliteStorageError::InvalidRowidKey(Key::Str("key".to_owned()));
    assert_eq!(actual, Err(expected.into()));

    // An explicit rowid of 20 is followed by SQLite's automatically assigned 21.
    storage
        .insert_data(
            "rows",
            vec![(Key::I64(20), vec![Value::I64(1), Value::I64(2)])],
        )
        .unwrap();
    storage
        .append_data("rows", vec![vec![Value::I64(3), Value::I64(4)]])
        .unwrap();
    assert_eq!(
        storage.fetch_data("rows", &Key::I64(20)).unwrap(),
        Some(vec![Value::I64(1), Value::I64(2)])
    );
    assert_eq!(
        storage.fetch_data("rows", &Key::I64(21)).unwrap(),
        Some(vec![Value::I64(3), Value::I64(4)])
    );

    // Deletion also uses oid, ignores a missing row, and preserves the other row.
    storage
        .delete_data("rows", vec![Key::I64(20), Key::I64(999)])
        .unwrap();
    assert_eq!(storage.fetch_data("rows", &Key::I64(20)).unwrap(), None);
    assert_eq!(
        storage.fetch_data("rows", &Key::I64(21)).unwrap(),
        Some(vec![Value::I64(3), Value::I64(4)])
    );
}

#[test]
fn selected_columns_must_match_the_schema() {
    let mut storage = SqliteStorage::in_memory().unwrap();
    let schema = Schema::from_ddl("CREATE TABLE item (id INT PRIMARY KEY, name TEXT)").unwrap();
    storage.insert_schema(&schema).unwrap();

    for (case, projection) in [
        ("column name does not match", vec![("wrong".to_owned(), 0)]),
        (
            "duplicate column",
            vec![("id".to_owned(), 0), ("id".to_owned(), 0)],
        ),
        (
            "columns are out of schema order",
            vec![("name".to_owned(), 1), ("id".to_owned(), 0)],
        ),
        (
            "column index is out of bounds",
            vec![("name".to_owned(), 2)],
        ),
    ] {
        let actual = storage
            .scan_data("item", ScanColumns::Selected(&projection))
            .err();
        let expected = Some(SqliteStorageError::InvalidProjection("item".to_owned()).into());
        assert_eq!(actual, expected, "{case}");
    }

    // Document tables have no column projection, including an empty selection.
    let schema = Schema::from_ddl("CREATE TABLE docs").unwrap();
    storage.insert_schema(&schema).unwrap();
    let actual = storage.scan_data("docs", ScanColumns::Selected(&[])).err();
    let expected = Some(SqliteStorageError::InvalidProjection("docs".to_owned()).into());
    assert_eq!(actual, expected);
}

#[test]
fn logical_type_metadata_must_be_canonical_and_match_the_physical_column() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("metadata.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let schema = Schema::from_ddl("CREATE TABLE item (id INT)").unwrap();
    storage.insert_schema(&schema).unwrap();

    let sqlite = Connection::open(&path).unwrap();
    // Bypass SQLite's metadata CHECK so fetch_schema must validate the stored tag.
    sqlite
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .unwrap();

    for (case, tag) in [
        ("unknown type name", "NOT_A_TYPE"),
        (
            "SQL alias instead of the canonical INT metadata tag",
            "INTEGER",
        ),
    ] {
        sqlite
            .execute(
                "UPDATE __gluesql_column_meta SET logical_type = ?1
                 WHERE table_name = 'item' AND column_name = 'id'",
                [tag],
            )
            .unwrap();
        let actual = storage.fetch_schema("item");
        let expected = SqliteStorageError::InvalidLogicalType(tag.to_owned());
        assert_eq!(actual, Err(expected.into()), "{case}");
    }

    // TEXT is canonical, but metadata cannot change the physical INT column.
    sqlite
        .execute(
            "UPDATE __gluesql_column_meta SET logical_type = 'TEXT'
             WHERE table_name = 'item' AND column_name = 'id'",
            [],
        )
        .unwrap();
    let actual = storage.fetch_schema("item");
    let expected = SqliteStorageError::ConflictColumnType {
        expected: DataType::Text,
        actual: "INT".to_owned(),
    };
    assert_eq!(actual, Err(expected.into()));
}

#[test]
fn invalid_default_metadata_preserves_the_translation_error() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("default.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let schema = Schema::from_ddl("CREATE TABLE item (id INT)").unwrap();
    storage.insert_schema(&schema).unwrap();

    // The column and its logical type are valid; only the stored expression is invalid.
    let sqlite = Connection::open(&path).unwrap();
    sqlite
        .execute(
            "UPDATE __gluesql_column_meta SET default_sql = 'CAST(1 AS BADTYPE)'
             WHERE table_name = 'item' AND column_name = 'id'",
            [],
        )
        .unwrap();

    let actual = storage.fetch_schema("item");
    let expected = Error::Translate(TranslateError::UnsupportedDataType("BADTYPE".to_owned()));
    assert_eq!(actual, Err(expected));
}

#[test]
fn schemaful_tables_allow_absent_but_not_schemaless_metadata() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("table-metadata.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let schema = Schema::from_ddl("CREATE TABLE item (id INT)").unwrap();
    storage.insert_schema(&schema).unwrap();

    // A document table needs a single _doc MAP column, not this ordinary id column.
    let sqlite = Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            "INSERT INTO __gluesql_table_meta (table_name, is_schemaless)
             VALUES ('item', 1)",
        )
        .unwrap();
    assert_eq!(
        storage.fetch_schema("item"),
        Err(SqliteStorageError::ConflictSchemalessSchema("item".to_owned()).into())
    );

    // Removing the incorrect document marker restores the original column schema.
    sqlite
        .execute(
            "DELETE FROM __gluesql_table_meta WHERE table_name = 'item'",
            [],
        )
        .unwrap();
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(schema.clone()));

    // Without a comment or document marker to restore, the metadata table is optional too.
    sqlite
        .execute_batch("DROP TABLE __gluesql_table_meta")
        .unwrap();
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(schema));
}

#[test]
fn column_type_metadata_is_required() {
    for (case, remove_metadata) in [
        (
            "column metadata row is missing",
            "DELETE FROM __gluesql_column_meta WHERE table_name = 'item' AND column_name = 'id'",
        ),
        (
            "column metadata table is missing",
            "DROP TABLE __gluesql_column_meta",
        ),
    ] {
        let directory = tempdir().unwrap();
        let path = directory.path().join("column-metadata.sqlite");
        let mut storage = SqliteStorage::new(&path).unwrap();
        let schema = Schema::from_ddl("CREATE TABLE item (id INT)").unwrap();
        storage.insert_schema(&schema).unwrap();

        // Physical INT alone cannot recover GlueSQL's logical type (INT8, INT, etc.).
        let sqlite = Connection::open(&path).unwrap();
        sqlite.execute_batch(remove_metadata).unwrap();

        let actual = storage.fetch_schema("item");
        let expected = SqliteStorageError::MissingLogicalType {
            table: "item".to_owned(),
            column: "id".to_owned(),
        };
        assert_eq!(actual, Err(expected.into()), "{case}");
    }
}

#[test]
fn conflicting_metadata_definitions_are_rejected_without_overwriting_them() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("collision.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let sqlite = Connection::open(&path).unwrap();

    // An unrelated table already occupies the name needed for column metadata.
    sqlite
        .execute_batch(
            "CREATE TABLE __gluesql_column_meta (original TEXT);
             INSERT INTO __gluesql_column_meta VALUES ('keep')",
        )
        .unwrap();

    let schema = Schema::from_ddl("CREATE TABLE item (id INT)").unwrap();
    let actual = storage.insert_schema(&schema);
    let expected = SqliteStorageError::ConflictMetadataSchema("__gluesql_column_meta".to_owned());
    assert_eq!(actual, Err(expected.into()));
    assert_eq!(storage.fetch_schema("item").unwrap(), None);

    // Failure must leave the pre-existing table and its contents intact.
    let value: String = sqlite
        .query_row("SELECT original FROM __gluesql_column_meta", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(value, "keep");

    // The table metadata created before discovering the collision must also roll back.
    let table_metadata_count: i64 = sqlite
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name = '__gluesql_table_meta'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(table_metadata_count, 0);
}

#[test]
fn reading_schema_rejects_altered_metadata_definitions() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("altered-metadata.sqlite");
    let mut storage = SqliteStorage::new(&path).unwrap();
    let schema = Schema::from_ddl("CREATE TABLE item (id INT)").unwrap();
    storage.insert_schema(&schema).unwrap();

    // Metadata created by GlueSQL is no longer trusted after its layout changes externally.
    let sqlite = Connection::open(&path).unwrap();
    sqlite
        .execute_batch("ALTER TABLE __gluesql_column_meta ADD COLUMN extra TEXT")
        .unwrap();

    let actual = storage.fetch_schema("item");
    let expected = SqliteStorageError::ConflictMetadataSchema("__gluesql_column_meta".to_owned());
    assert_eq!(actual, Err(expected.into()));
}

#[test]
fn opening_a_directory_reports_the_sqlite_failure() {
    let directory = tempdir().unwrap();

    assert!(matches!(
        SqliteStorage::new(directory.path()),
        Err(SqliteStorageError::Sqlite(rusqlite::Error::SqliteFailure(error, _)))
            if error.code == rusqlite::ErrorCode::CannotOpen
    ));
}
