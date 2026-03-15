use {
    bincode::{deserialize, serialize},
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Error,
        store::{Store, StoreMut, Transaction},
    },
    gluesql_redb_storage::{REDB_STORAGE_FORMAT_VERSION, RedbStorage, migrate_to_latest},
    redb::{Database, ReadableTable, TableDefinition},
    serde::Serialize,
    std::{
        collections::BTreeMap,
        fs::{create_dir, create_dir_all, remove_dir_all, remove_file},
    },
    uuid::Uuid,
};

const SCHEMA_TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("__SCHEMA__");
const META_TABLE: TableDefinition<&str, u32> = TableDefinition::new("__GLUESQL_META__");
const META_TABLE_WITH_BINARY_VALUE: TableDefinition<&str, Vec<u8>> =
    TableDefinition::new("__GLUESQL_META__");
const SCHEMA_TABLE_WITH_BINARY_KEY: TableDefinition<&[u8], Vec<u8>> =
    TableDefinition::new("__SCHEMA__");
const STORAGE_META_VERSION_KEY: &str = "storage_format_version";

#[derive(Serialize)]
enum V1DataRow {
    Vec(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

fn test_path(name: &str) -> String {
    format!("tmp/{name}-{}", Uuid::now_v7())
}

fn remove_path(path: &str) {
    if let Err(err) = remove_file(path) {
        eprintln!("remove_file error: {err:?}");
    }
    if let Err(err) = remove_dir_all(path) {
        eprintln!("remove_dir_all error: {err:?}");
    }
}

fn write_v1_storage(path: &str, table_name: &str, rows: Vec<(Key, V1DataRow)>) {
    let _ = create_dir("tmp");
    remove_path(path);

    let db = Database::create(path).expect("create database");
    let txn = db.begin_write().expect("begin write transaction");

    let schema = Schema {
        table_name: table_name.to_owned(),
        column_defs: None,
        indexes: Vec::new(),
        engine: None,
        foreign_keys: Vec::new(),
        comment: None,
    };
    let mut schema_table = txn.open_table(SCHEMA_TABLE).expect("open schema table");
    schema_table
        .insert(table_name, serialize(&schema).expect("serialize schema"))
        .expect("insert schema");
    drop(schema_table);

    let table_def = TableDefinition::<&[u8], Vec<u8>>::new(table_name);
    let mut table = txn.open_table(table_def).expect("open data table");
    for (key, row) in rows {
        let table_key = key.to_cmp_be_bytes().expect("table key bytes");
        let payload = serialize(&(&key, row)).expect("serialize v1 row");
        table
            .insert(table_key.as_slice(), payload)
            .expect("insert v1 row");
    }
    drop(table);

    txn.commit().expect("commit");
}

fn read_v2_rows(path: &str, table_name: &str) -> Vec<(Key, Vec<Value>)> {
    let db = Database::create(path).expect("open database");
    let txn = db.begin_read().expect("begin read transaction");
    let table_def = TableDefinition::<&[u8], Vec<u8>>::new(table_name);
    let table = txn.open_table(table_def).expect("open data table");

    table
        .iter()
        .expect("iterate rows")
        .map(|entry| {
            let value = entry.expect("row entry").1.value();
            deserialize::<(Key, Vec<Value>)>(&value).expect("deserialize v2 row")
        })
        .collect()
}

fn set_storage_format_version(path: &str, version: Option<u32>) {
    let db = Database::create(path).expect("open database");
    let txn = db.begin_write().expect("begin write transaction");
    let mut table = txn.open_table(META_TABLE).expect("open metadata table");

    if let Some(version) = version {
        table
            .insert(STORAGE_META_VERSION_KEY, &version)
            .expect("insert metadata");
    } else {
        table
            .remove(STORAGE_META_VERSION_KEY)
            .expect("remove metadata");
    }
    drop(table);

    txn.commit().expect("commit");
}

fn insert_raw_row_payload(path: &str, table_name: &str, key: &Key, payload: Vec<u8>) {
    let db = Database::create(path).expect("open database");
    let txn = db.begin_write().expect("begin write transaction");
    let table_def = TableDefinition::<&[u8], Vec<u8>>::new(table_name);
    let mut table = txn.open_table(table_def).expect("open table");
    let table_key = key.to_cmp_be_bytes().expect("table key bytes");
    table
        .insert(table_key.as_slice(), payload)
        .expect("insert payload");
    drop(table);
    txn.commit().expect("commit");
}

fn read_storage_format_version(path: &str) -> Option<u32> {
    let db = Database::create(path).expect("open database");
    let txn = db.begin_read().expect("begin read transaction");

    match txn.open_table(META_TABLE) {
        Ok(table) => table
            .get(STORAGE_META_VERSION_KEY)
            .expect("read metadata")
            .map(|value| value.value()),
        Err(redb::TableError::TableDoesNotExist(_)) => None,
        Err(err) => panic!("unexpected metadata table error: {err}"),
    }
}

#[tokio::test]
async fn store_methods_without_transaction_return_transaction_not_found() {
    let path = test_path("redb-transaction-not-found");
    let mut storage = RedbStorage::new(&path).expect("new storage");

    let err = storage
        .fetch_all_schemas()
        .await
        .expect_err("fetch_all_schemas should fail");
    assert_eq!(err, Error::StorageMsg("transaction not found".to_owned()));

    let schema = Schema::from_ddl("CREATE TABLE Foo (id INTEGER);").expect("parse schema");
    let err = storage
        .insert_schema(&schema)
        .await
        .expect_err("insert_schema should fail");
    assert_eq!(err, Error::StorageMsg("transaction not found".to_owned()));

    storage.rollback().await.expect("rollback without txn");
    storage.commit().await.expect("commit without txn");

    remove_path(&path);
}

#[test]
fn new_storage_initializes_format_version() {
    let path = test_path("redb-version-init");
    RedbStorage::new(&path).expect("new storage");

    assert_eq!(
        read_storage_format_version(&path),
        Some(REDB_STORAGE_FORMAT_VERSION),
    );

    remove_path(&path);
}

#[test]
fn v1_storage_requires_migration() {
    let path = test_path("redb-v1-requires-migration");
    write_v1_storage(
        &path,
        "Foo",
        vec![(Key::I64(1), V1DataRow::Vec(vec![Value::I64(1)]))],
    );

    let err = match RedbStorage::new(&path) {
        Ok(_storage) => panic!("migration should be required"),
        Err(err) => err,
    };
    assert_eq!(
        err,
        Error::StorageMsg(
            "[RedbStorage] migration required (found v1, expected v2); migrate redb-storage data to the latest format before opening"
                .to_owned(),
        ),
    );

    remove_path(&path);
}

#[test]
fn v1_to_v2_migration_rewrites_rows_and_is_idempotent() {
    let path = test_path("redb-v1-to-v2");
    write_v1_storage(
        &path,
        "Foo",
        vec![
            (
                Key::I64(1),
                V1DataRow::Map(BTreeMap::from([("id".to_owned(), Value::I64(7))])),
            ),
            (Key::I64(2), V1DataRow::Vec(vec![Value::I64(10)])),
        ],
    );

    let report = migrate_to_latest(&path).expect("migrate to latest");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.unchanged_tables, 0);
    assert_eq!(report.rewritten_rows, 2);
    assert_eq!(
        read_storage_format_version(&path),
        Some(REDB_STORAGE_FORMAT_VERSION),
    );

    let rows = read_v2_rows(&path, "Foo");
    assert!(rows.iter().any(|(key, row)| {
        key == &Key::I64(1)
            && row
                == &vec![Value::Map(BTreeMap::from([(
                    "id".to_owned(),
                    Value::I64(7),
                )]))]
    }));
    assert!(
        rows.iter()
            .any(|(key, row)| key == &Key::I64(2) && row == &vec![Value::I64(10)])
    );

    let second = migrate_to_latest(&path).expect("second migration");
    assert_eq!(second.migrated_tables, 0);
    assert_eq!(second.unchanged_tables, 1);
    assert_eq!(second.rewritten_rows, 0);

    RedbStorage::new(&path).expect("new storage after migration");

    remove_path(&path);
}

#[test]
fn v1_migration_rewrites_large_table() {
    let path = test_path("redb-v1-large-migration");
    let rows = (0..4500_i64)
        .map(|id| {
            (
                Key::I64(id),
                V1DataRow::Map(BTreeMap::from([("id".to_owned(), Value::I64(id))])),
            )
        })
        .collect();
    write_v1_storage(&path, "Foo", rows);

    let report = migrate_to_latest(&path).expect("migrate large v1 db");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.unchanged_tables, 0);
    assert_eq!(report.rewritten_rows, 4500);

    let rows = read_v2_rows(&path, "Foo");
    assert_eq!(rows.len(), 4500);
    assert!(rows.iter().any(|(key, row)| {
        key == &Key::I64(4096)
            && row
                == &vec![Value::Map(BTreeMap::from([(
                    "id".to_owned(),
                    Value::I64(4096),
                )]))]
    }));

    remove_path(&path);
}

#[test]
fn newer_version_is_rejected() {
    let path = test_path("redb-newer-version");
    RedbStorage::new(&path).expect("new storage");
    set_storage_format_version(&path, Some(REDB_STORAGE_FORMAT_VERSION + 1));

    let err = match RedbStorage::new(&path) {
        Ok(_storage) => panic!("newer version should fail"),
        Err(err) => err,
    };
    assert_eq!(
        err,
        Error::StorageMsg(format!(
            "[RedbStorage] unsupported newer format version v{}",
            REDB_STORAGE_FORMAT_VERSION + 1,
        )),
    );

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert_eq!(
        err,
        Error::StorageMsg(format!(
            "[RedbStorage] unsupported newer format version v{}",
            REDB_STORAGE_FORMAT_VERSION + 1,
        )),
    );

    remove_path(&path);
}

#[test]
fn unsupported_v0_version_is_rejected() {
    let path = test_path("redb-unsupported-v0");
    RedbStorage::new(&path).expect("new storage");
    set_storage_format_version(&path, Some(0));

    let err = match RedbStorage::new(&path) {
        Ok(_storage) => panic!("unsupported v0 should fail"),
        Err(err) => err,
    };
    assert_eq!(
        err,
        Error::StorageMsg("[RedbStorage] unsupported format version v0".to_owned()),
    );

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert_eq!(
        err,
        Error::StorageMsg("[RedbStorage] unsupported format version v0".to_owned()),
    );

    remove_path(&path);
}

#[test]
fn missing_metadata_key_is_rejected() {
    let path = test_path("redb-missing-metadata");
    RedbStorage::new(&path).expect("new storage");
    set_storage_format_version(&path, None);

    let err = match RedbStorage::new(&path) {
        Ok(_storage) => panic!("missing version key should fail"),
        Err(err) => err,
    };
    assert_eq!(
        err,
        Error::StorageMsg(
            "[RedbStorage] invalid storage format metadata: missing format version key".to_owned(),
        ),
    );

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert_eq!(
        err,
        Error::StorageMsg(
            "[RedbStorage] invalid storage format metadata: missing format version key".to_owned(),
        ),
    );

    remove_path(&path);
}

#[test]
fn migration_path_must_exist() {
    let path = test_path("redb-missing-path");
    let err = migrate_to_latest(&path).expect_err("missing path should fail");
    assert_eq!(
        err,
        Error::StorageMsg(format!(
            "[RedbStorage] storage path '{path}' does not exist"
        )),
    );
}

#[test]
fn migration_path_must_be_file() {
    let path = test_path("redb-directory-path");
    create_dir_all(&path).expect("create directory path");

    let err = migrate_to_latest(&path).expect_err("directory path should fail");
    assert_eq!(
        err,
        Error::StorageMsg(format!("[RedbStorage] storage path '{path}' is not a file")),
    );

    remove_path(&path);
}

#[test]
fn migrate_empty_storage_file_initializes_format_version() {
    let path = test_path("redb-empty-migrate");
    let _ = create_dir("tmp");
    remove_path(&path);
    Database::create(&path).expect("create empty db");

    let report = migrate_to_latest(&path).expect("migrate empty db");
    assert_eq!(report.migrated_tables, 0);
    assert_eq!(report.unchanged_tables, 0);
    assert_eq!(report.rewritten_rows, 0);
    assert_eq!(
        read_storage_format_version(&path),
        Some(REDB_STORAGE_FORMAT_VERSION),
    );

    remove_path(&path);
}

#[test]
fn v1_migration_keeps_rows_already_in_v2_shape() {
    let path = test_path("redb-v1-v2-shaped-row");
    write_v1_storage(&path, "Foo", vec![]);

    let key = Key::I64(7);
    let payload = serialize(&(&key, vec![Value::I64(100)])).expect("serialize v2 row payload");
    insert_raw_row_payload(&path, "Foo", &key, payload);

    let report = migrate_to_latest(&path).expect("migrate v1 db");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.unchanged_tables, 0);
    assert_eq!(report.rewritten_rows, 0);

    let rows = read_v2_rows(&path, "Foo");
    assert!(
        rows.iter()
            .any(|(actual_key, row)| actual_key == &key && row == &vec![Value::I64(100)])
    );

    remove_path(&path);
}

#[test]
fn invalid_v1_row_payload_returns_error() {
    let path = test_path("redb-invalid-v1-row");
    write_v1_storage(&path, "Foo", vec![]);

    let key = Key::I64(9);
    insert_raw_row_payload(&path, "Foo", &key, vec![1, 2, 3, 4]);

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert_eq!(
        err,
        Error::StorageMsg("[RedbStorage] failed to parse v1 row payload in table 'Foo'".to_owned()),
    );

    remove_path(&path);
}

#[test]
fn migrate_v2_storage_without_schema_table_is_unchanged() {
    let path = test_path("redb-v2-without-schema-table");
    RedbStorage::new(&path).expect("new storage");

    let report = migrate_to_latest(&path).expect("migrate v2 storage without schema table");
    assert_eq!(report.migrated_tables, 0);
    assert_eq!(report.unchanged_tables, 0);
    assert_eq!(report.rewritten_rows, 0);

    remove_path(&path);
}

#[test]
fn invalid_schema_table_type_is_rejected() {
    let path = test_path("redb-invalid-schema-table-type");
    let _ = create_dir("tmp");
    remove_path(&path);

    let db = Database::create(&path).expect("create database");
    let txn = db.begin_write().expect("begin write transaction");
    let mut wrong_schema_table = txn
        .open_table(SCHEMA_TABLE_WITH_BINARY_KEY)
        .expect("open schema table with unexpected key type");
    wrong_schema_table
        .insert(
            b"Foo".as_slice(),
            serialize(&1_i64).expect("serialize dummy value"),
        )
        .expect("insert row into wrong schema table");
    drop(wrong_schema_table);

    let mut meta_table = txn.open_table(META_TABLE).expect("open metadata table");
    meta_table
        .insert(STORAGE_META_VERSION_KEY, &REDB_STORAGE_FORMAT_VERSION)
        .expect("insert format version");
    drop(meta_table);
    txn.commit().expect("commit");
    drop(db);

    let expected = {
        let db = Database::open(&path).expect("open database");
        let txn = db.begin_read().expect("begin read transaction");
        let err = txn
            .open_table(SCHEMA_TABLE)
            .expect_err("schema table type mismatch should fail");
        Error::StorageMsg(err.to_string())
    };

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert_eq!(err, expected);

    remove_path(&path);
}

#[test]
fn invalid_metadata_table_type_is_rejected() {
    let path = test_path("redb-invalid-metadata-table-type");
    let _ = create_dir("tmp");
    remove_path(&path);

    let db = Database::create(&path).expect("create database");
    let txn = db.begin_write().expect("begin write transaction");
    let mut wrong_meta_table = txn
        .open_table(META_TABLE_WITH_BINARY_VALUE)
        .expect("open metadata table with unexpected value type");
    wrong_meta_table
        .insert(STORAGE_META_VERSION_KEY, vec![2_u8, 0, 0, 0])
        .expect("insert row into wrong metadata table");
    drop(wrong_meta_table);
    txn.commit().expect("commit");
    drop(db);

    let expected = {
        let db = Database::open(&path).expect("open database");
        let txn = db.begin_read().expect("begin read transaction");
        let err = txn
            .open_table(META_TABLE)
            .expect_err("metadata table type mismatch should fail");
        Error::StorageMsg(err.to_string())
    };

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert_eq!(err, expected);

    remove_path(&path);
}

#[test]
fn invalid_v1_schema_table_type_is_rejected() {
    let path = test_path("redb-invalid-v1-schema-table-type");
    let _ = create_dir("tmp");
    remove_path(&path);

    let db = Database::create(&path).expect("create database");
    let txn = db.begin_write().expect("begin write transaction");
    let mut wrong_schema_table = txn
        .open_table(SCHEMA_TABLE_WITH_BINARY_KEY)
        .expect("open schema table with unexpected key type");
    wrong_schema_table
        .insert(
            b"Foo".as_slice(),
            serialize(&1_i64).expect("serialize dummy value"),
        )
        .expect("insert row into wrong schema table");
    drop(wrong_schema_table);
    txn.commit().expect("commit");
    drop(db);

    let expected = {
        let db = Database::open(&path).expect("open database");
        let txn = db.begin_write().expect("begin write transaction");
        let err = txn
            .open_table(SCHEMA_TABLE)
            .expect_err("schema table type mismatch should fail");
        Error::StorageMsg(err.to_string())
    };

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert_eq!(err, expected);

    remove_path(&path);
}
