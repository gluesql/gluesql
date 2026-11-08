use {
    gluesql_core::{
        data::{Key, KeyError, Schema, Value},
        error::{Error, Result},
        prelude::Glue,
        store::{Store, StoreMut, Transaction},
    },
    gluesql_slatedb_storage::{SlatedbStorage, object_store::memory::InMemory},
    std::sync::Arc,
    tokio::runtime::Builder,
};

#[test]
fn opening_the_same_memory_url_creates_a_fresh_database() {
    let storage = SlatedbStorage::open("memory:///my_database").unwrap();
    let mut glue = Glue::new(storage);
    glue.execute(
        r"
        CREATE TABLE Items (id INTEGER PRIMARY KEY);
        INSERT INTO Items VALUES (1);
        ",
    )
    .unwrap();

    assert_eq!(
        glue.storage.fetch_data("Items", &Key::I64(1)),
        Ok(Some(vec![Value::I64(1)]))
    );
    glue.storage.close().unwrap();

    let storage = SlatedbStorage::open("memory:///my_database").unwrap();
    assert_eq!(storage.fetch_all_schemas(), Ok(vec![]));
    assert_eq!(storage.fetch_data("Items", &Key::I64(1)), Ok(None));
    storage.close().unwrap();
}

#[test]
fn open_rejects_a_relative_url() {
    assert_eq!(
        SlatedbStorage::open("my_database").err(),
        Some(Error::StorageMsg(
            "failed to parse storage URL: relative URL without a base".into()
        ))
    );
}

#[test]
fn open_reports_an_unsupported_object_store_scheme() {
    assert_eq!(
        SlatedbStorage::open("unknown:///my_database").err(),
        Some(Error::StorageMsg(
            r#"failed to configure object store: Generic URL error: Unable to recognise URL "unknown:///my_database""#.into()
        ))
    );
}

#[test]
fn table_names_survive_reopen() {
    let objects = Arc::new(InMemory::new());
    let mut storage = SlatedbStorage::new(objects.clone(), "table_names").unwrap();
    let table_names = ["users", "users2", "schema", "data", "한글"];

    storage.begin(false).unwrap();
    for name in table_names {
        let schema = Schema::from_ddl(&format!(r#"CREATE TABLE "{name}" (value TEXT)"#)).unwrap();
        storage.insert_schema(&schema).unwrap();
        storage
            .insert_data(name, vec![(Key::I64(1), vec![Value::Str(name.into())])])
            .unwrap();
    }
    storage.commit().unwrap();
    storage.close().unwrap();

    let storage = SlatedbStorage::new(objects, "table_names").unwrap();
    for name in table_names {
        let expected = Schema::from_ddl(&format!(r#"CREATE TABLE "{name}" (value TEXT)"#)).unwrap();
        assert_eq!(storage.fetch_schema(name), Ok(Some(expected)));
        assert_eq!(
            storage.scan_data(name).unwrap().collect::<Result<Vec<_>>>(),
            Ok(vec![(Key::I64(1), vec![Value::Str(name.into())])])
        );
    }
    storage.close().unwrap();
}

#[test]
fn row_keys_with_slashes_survive_reopen() {
    let objects = Arc::new(InMemory::new());
    let mut storage = SlatedbStorage::new(objects.clone(), "row_keys").unwrap();
    let schema = Schema::from_ddl("CREATE TABLE Items (value TEXT)").unwrap();
    let string_key = Key::Str("a/b".into());
    let binary_key = Key::Bytea(b"/\0\xff".to_vec());

    storage.begin(false).unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .insert_data(
            "Items",
            vec![
                (string_key.clone(), vec![Value::Str("string key".into())]),
                (binary_key.clone(), vec![Value::Str("binary key".into())]),
            ],
        )
        .unwrap();
    storage.commit().unwrap();
    storage.close().unwrap();

    let storage = SlatedbStorage::new(objects, "row_keys").unwrap();
    assert_eq!(
        storage.fetch_data("Items", &string_key),
        Ok(Some(vec![Value::Str("string key".into())]))
    );
    assert_eq!(
        storage.fetch_data("Items", &binary_key),
        Ok(Some(vec![Value::Str("binary key".into())]))
    );
    assert_eq!(
        storage
            .scan_data("Items")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![
            (string_key, vec![Value::Str("string key".into())]),
            (binary_key, vec![Value::Str("binary key".into())]),
        ])
    );
    storage.close().unwrap();
}

#[test]
fn database_prefixes_isolate_schemas_and_rows() {
    let objects = Arc::new(InMemory::new());
    let storage = SlatedbStorage::new(objects.clone(), "first_database").unwrap();
    let mut glue = Glue::new(storage);
    glue.execute(
        r"
        CREATE TABLE Items (id INTEGER PRIMARY KEY);
        INSERT INTO Items VALUES (1);
        ",
    )
    .unwrap();

    let other = SlatedbStorage::new(objects, "second_database").unwrap();
    assert_eq!(other.fetch_all_schemas(), Ok(vec![]));
    assert_eq!(other.fetch_schema("Items"), Ok(None));
    assert_eq!(other.fetch_data("Items", &Key::I64(1)), Ok(None));
    assert_eq!(
        other
            .scan_data("Items")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![])
    );

    other.close().unwrap();
    glue.storage.close().unwrap();
}

#[test]
fn dropping_table_clears_only_its_own_prefix() {
    let objects = Arc::new(InMemory::new());
    let storage = SlatedbStorage::new(objects.clone(), "drop_table").unwrap();
    let mut glue = Glue::new(storage);
    glue.execute(
        r"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        CREATE TABLE users2 (id INTEGER PRIMARY KEY);
        INSERT INTO users VALUES (1);
        INSERT INTO users2 VALUES (2);
        ",
    )
    .unwrap();

    glue.execute("DROP TABLE users").unwrap();
    assert_eq!(glue.storage.fetch_schema("users"), Ok(None));
    glue.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
        .unwrap();
    glue.storage.close().unwrap();

    let storage = SlatedbStorage::new(objects, "drop_table").unwrap();
    assert_eq!(
        storage
            .scan_data("users")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![])
    );
    assert_eq!(
        storage
            .scan_data("users2")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![(Key::I64(2), vec![Value::I64(2)])])
    );
    storage.close().unwrap();
}

#[test]
fn schema_update_preserves_rows_after_reopen() {
    let objects = Arc::new(InMemory::new());
    let mut storage = SlatedbStorage::new(objects.clone(), "schema_update").unwrap();
    let mut schema = Schema::from_ddl("CREATE TABLE Items (id INTEGER)").unwrap();

    storage.begin(false).unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .insert_data("Items", vec![(Key::I64(1), vec![Value::I64(1)])])
        .unwrap();

    schema.comment = Some("updated".into());
    storage.insert_schema(&schema).unwrap();
    assert_eq!(storage.fetch_all_schemas(), Ok(vec![schema.clone()]));
    storage.commit().unwrap();
    storage.close().unwrap();

    let storage = SlatedbStorage::new(objects, "schema_update").unwrap();
    assert_eq!(storage.fetch_schema("Items"), Ok(Some(schema)));
    assert_eq!(
        storage
            .scan_data("Items")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![(Key::I64(1), vec![Value::I64(1)])])
    );
    storage.close().unwrap();
}

#[test]
fn identical_rows_get_distinct_uuid_keys_that_survive_reopen() {
    let objects = Arc::new(InMemory::new());
    let mut storage = SlatedbStorage::new(objects.clone(), "uuid_keys").unwrap();
    let schema = Schema::from_ddl("CREATE TABLE Items (value INTEGER)").unwrap();

    storage.begin(false).unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .append_data("Items", vec![vec![Value::I64(1)], vec![Value::I64(1)]])
        .unwrap();

    let rows = storage
        .scan_data("Items")
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0].0, rows[1].0);
    for (key, row) in &rows {
        assert!(matches!(key, Key::Uuid(_)));
        assert_eq!(row, &vec![Value::I64(1)]);
        assert_eq!(
            storage.fetch_data("Items", key),
            Ok(Some(vec![Value::I64(1)]))
        );
    }
    let first_key = rows[0].0.clone();
    let second_key = rows[1].0.clone();
    storage.commit().unwrap();
    storage.close().unwrap();

    let storage = SlatedbStorage::new(objects, "uuid_keys").unwrap();
    assert_eq!(
        storage
            .scan_data("Items")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![
            (first_key, vec![Value::I64(1)]),
            (second_key, vec![Value::I64(1)]),
        ])
    );
    storage.close().unwrap();
}

#[test]
fn dropping_a_partly_consumed_scan_allows_further_writes() {
    let mut storage = SlatedbStorage::new(Arc::new(InMemory::new()), "partial_scan").unwrap();
    let schema = Schema::from_ddl("CREATE TABLE Items (id INTEGER)").unwrap();

    storage.begin(false).unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .insert_data(
            "Items",
            vec![
                (Key::I64(1), vec![Value::I64(1)]),
                (Key::I64(2), vec![Value::I64(2)]),
            ],
        )
        .unwrap();

    let mut rows = storage.scan_data("Items").unwrap();
    assert_eq!(rows.next(), Some(Ok((Key::I64(1), vec![Value::I64(1)]))));
    drop(rows);

    storage.delete_data("Items", vec![Key::I64(1)]).unwrap();
    assert_eq!(storage.fetch_data("Items", &Key::I64(1)), Ok(None));
    storage.commit().unwrap();
    assert_eq!(
        storage
            .scan_data("Items")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![(Key::I64(2), vec![Value::I64(2)])])
    );
    storage.close().unwrap();
}

#[test]
fn committed_alter_changes_survive_reopen() {
    let objects = Arc::new(InMemory::new());
    let storage = SlatedbStorage::new(objects.clone(), "alter_commit").unwrap();
    let mut glue = Glue::new(storage);
    glue.execute(
        r"
        CREATE TABLE Items (id INTEGER PRIMARY KEY);
        INSERT INTO Items VALUES (1);
        ",
    )
    .unwrap();

    glue.execute(
        r"
        BEGIN;
        ALTER TABLE Items RENAME TO Renamed;
        ALTER TABLE Renamed ADD COLUMN count INTEGER DEFAULT 7;
        COMMIT;
        ",
    )
    .unwrap();
    glue.storage.close().unwrap();

    let storage = SlatedbStorage::new(objects, "alter_commit").unwrap();
    let expected =
        Schema::from_ddl("CREATE TABLE Renamed (id INTEGER PRIMARY KEY, count INTEGER DEFAULT 7)")
            .unwrap();
    assert_eq!(storage.fetch_schema("Renamed"), Ok(Some(expected)));
    assert_eq!(storage.fetch_schema("Items"), Ok(None));
    assert_eq!(
        storage
            .scan_data("Items")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![])
    );
    assert_eq!(
        storage
            .scan_data("Renamed")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![(Key::I64(1), vec![Value::I64(1), Value::I64(7)])])
    );
    storage.close().unwrap();
}

#[test]
fn rolled_back_rename_leaves_no_new_prefix_after_reopen() {
    let objects = Arc::new(InMemory::new());
    let storage = SlatedbStorage::new(objects.clone(), "alter_rollback").unwrap();
    let mut glue = Glue::new(storage);
    glue.execute(
        r"
        CREATE TABLE Items (id INTEGER PRIMARY KEY);
        INSERT INTO Items VALUES (1);
        ",
    )
    .unwrap();

    glue.execute(
        r"
        BEGIN;
        ALTER TABLE Items RENAME TO Discarded;
        ROLLBACK;
        ",
    )
    .unwrap();
    glue.storage.close().unwrap();

    let storage = SlatedbStorage::new(objects, "alter_rollback").unwrap();
    let expected = Schema::from_ddl("CREATE TABLE Items (id INTEGER PRIMARY KEY)").unwrap();
    assert_eq!(storage.fetch_schema("Items"), Ok(Some(expected)));
    assert_eq!(storage.fetch_schema("Discarded"), Ok(None));
    assert_eq!(
        storage
            .scan_data("Discarded")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![])
    );
    assert_eq!(
        storage
            .scan_data("Items")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![(Key::I64(1), vec![Value::I64(1)])])
    );
    storage.close().unwrap();
}

#[test]
fn close_discards_uncommitted_rows() {
    let objects = Arc::new(InMemory::new());
    let storage = SlatedbStorage::new(objects.clone(), "close").unwrap();
    let mut glue = Glue::new(storage);
    glue.execute(
        r"
        CREATE TABLE Items (id INTEGER PRIMARY KEY);
        INSERT INTO Items VALUES (1);
        ",
    )
    .unwrap();

    glue.execute(
        r"
        BEGIN;
        INSERT INTO Items VALUES (2);
        ",
    )
    .unwrap();
    glue.storage.close().unwrap();

    let storage = SlatedbStorage::new(objects, "close").unwrap();
    assert_eq!(
        storage
            .scan_data("Items")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![(Key::I64(1), vec![Value::I64(1)])])
    );
    storage.close().unwrap();
}

#[test]
fn drop_discards_uncommitted_rows() {
    let objects = Arc::new(InMemory::new());
    let storage = SlatedbStorage::new(objects.clone(), "drop").unwrap();
    let mut glue = Glue::new(storage);
    glue.execute(
        r"
        CREATE TABLE Items (id INTEGER PRIMARY KEY);
        INSERT INTO Items VALUES (1);
        ",
    )
    .unwrap();

    glue.execute(
        r"
        BEGIN;
        INSERT INTO Items VALUES (2);
        ",
    )
    .unwrap();
    drop(glue);

    let storage = SlatedbStorage::new(objects, "drop").unwrap();
    assert_eq!(
        storage
            .scan_data("Items")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![(Key::I64(1), vec![Value::I64(1)])])
    );
    storage.close().unwrap();
}

#[test]
fn sql_rejects_slashes_in_table_names_without_changing_existing_data() {
    let objects = Arc::new(InMemory::new());
    let storage = SlatedbStorage::new(objects.clone(), "sql_names").unwrap();
    let mut glue = Glue::new(storage);
    glue.execute(
        r"
        CREATE TABLE Items (id INTEGER PRIMARY KEY);
        INSERT INTO Items VALUES (1);
        ",
    )
    .unwrap();

    let expected = Error::StorageMsg("table name cannot contain '/': a/b".into());
    assert_eq!(
        glue.execute(r#"CREATE TABLE "a/b" (id INTEGER)"#)
            .unwrap_err(),
        expected
    );
    assert_eq!(
        glue.execute(r#"ALTER TABLE Items RENAME TO "a/b""#)
            .unwrap_err(),
        expected
    );
    glue.storage.close().unwrap();

    let storage = SlatedbStorage::new(objects, "sql_names").unwrap();
    assert_eq!(
        storage.fetch_all_schemas(),
        Ok(vec![
            Schema::from_ddl("CREATE TABLE Items (id INTEGER PRIMARY KEY)").unwrap()
        ])
    );
    assert_eq!(
        storage
            .scan_data("Items")
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![(Key::I64(1), vec![Value::I64(1)])])
    );
    storage.close().unwrap();
}

#[test]
fn storage_methods_reject_slashes_in_table_names() {
    let mut storage = SlatedbStorage::new(Arc::new(InMemory::new()), "storage_names").unwrap();
    let schema = Schema::from_ddl(r#"CREATE TABLE "a/b" (id INTEGER)"#).unwrap();
    let expected = Error::StorageMsg("table name cannot contain '/': a/b".into());

    storage.begin(false).unwrap();
    assert_eq!(storage.insert_schema(&schema).unwrap_err(), expected);
    assert_eq!(storage.delete_schema("a/b").unwrap_err(), expected);
    assert_eq!(
        storage
            .append_data("a/b", vec![vec![Value::I64(1)]])
            .unwrap_err(),
        expected
    );
    assert_eq!(
        storage
            .insert_data("a/b", vec![(Key::I64(1), vec![Value::I64(1)])])
            .unwrap_err(),
        expected
    );
    assert_eq!(
        storage.delete_data("a/b", vec![Key::I64(1)]).unwrap_err(),
        expected
    );
    assert_eq!(storage.fetch_schema("a/b").unwrap_err(), expected);
    assert_eq!(
        storage.fetch_data("a/b", &Key::I64(1)).unwrap_err(),
        expected
    );
    assert_eq!(storage.scan_data("a/b").err(), Some(expected));
    storage.rollback().unwrap();
    storage.close().unwrap();
}

#[test]
fn writes_require_an_active_transaction() {
    let mut storage = SlatedbStorage::new(Arc::new(InMemory::new()), "missing_txn").unwrap();
    let schema = Schema::from_ddl("CREATE TABLE Items (id INTEGER)").unwrap();
    let expected = Error::StorageMsg("transaction not found".into());

    assert_eq!(storage.insert_schema(&schema).unwrap_err(), expected);
    assert_eq!(storage.delete_schema("Items").unwrap_err(), expected);
    assert_eq!(
        storage
            .append_data("Items", vec![vec![Value::I64(1)]])
            .unwrap_err(),
        expected
    );
    assert_eq!(
        storage
            .insert_data("Items", vec![(Key::I64(1), vec![Value::I64(1)])])
            .unwrap_err(),
        expected
    );
    assert_eq!(
        storage.delete_data("Items", vec![Key::I64(1)]).unwrap_err(),
        expected
    );
    storage.close().unwrap();
}

#[test]
fn begin_commit_and_rollback_update_transaction_state() {
    let mut storage = SlatedbStorage::new(Arc::new(InMemory::new()), "txn_state").unwrap();
    let schema = Schema::from_ddl("CREATE TABLE Items (id INTEGER)").unwrap();

    storage.commit().unwrap();
    storage.rollback().unwrap();

    assert_eq!(storage.begin(true), Ok(true));
    assert_eq!(storage.begin(true), Ok(false));
    assert_eq!(
        storage.begin(false),
        Err(Error::StorageMsg(
            "nested transaction is not supported".into()
        ))
    );
    storage.commit().unwrap();

    assert_eq!(storage.begin(false), Ok(false));
    storage.insert_schema(&schema).unwrap();
    storage.rollback().unwrap();
    assert_eq!(storage.fetch_schema("Items"), Ok(None));
    storage.close().unwrap();
}

#[test]
fn unsupported_row_keys_preserve_the_core_error() {
    let mut storage = SlatedbStorage::new(Arc::new(InMemory::new()), "unsupported_key").unwrap();
    let schema = Schema::from_ddl("CREATE TABLE Items (value INTEGER)").unwrap();
    let key = Key::F64(1.0.into());
    let expected = Error::Key(KeyError::FloatToCmpBigEndianNotSupported);

    storage.begin(false).unwrap();
    storage.insert_schema(&schema).unwrap();
    assert_eq!(storage.fetch_data("Items", &key).unwrap_err(), expected);
    assert_eq!(
        storage
            .insert_data("Items", vec![(key.clone(), vec![Value::I64(1)])])
            .unwrap_err(),
        expected
    );
    assert_eq!(
        storage.delete_data("Items", vec![key]).unwrap_err(),
        expected
    );
    storage.rollback().unwrap();
    storage.close().unwrap();
}

#[test]
fn async_host_uses_blocking_thread_for_the_whole_lifetime() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    runtime.block_on(async {
        tokio::task::spawn_blocking(|| {
            let objects = Arc::new(InMemory::new());
            let storage = SlatedbStorage::new(objects.clone(), "blocking").unwrap();
            let mut glue = Glue::new(storage);

            glue.execute(
                r"
                CREATE TABLE Items (id INTEGER PRIMARY KEY);
                INSERT INTO Items VALUES (1);
                ",
            )
            .unwrap();
            glue.storage.close().unwrap();

            let storage = SlatedbStorage::new(objects, "blocking").unwrap();
            assert_eq!(
                storage.fetch_data("Items", &Key::I64(1)),
                Ok(Some(vec![Value::I64(1)]))
            );
            drop(storage);
        })
        .await
        .unwrap();
    });
}
