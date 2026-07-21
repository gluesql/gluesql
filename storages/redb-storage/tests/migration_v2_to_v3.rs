use {
    bincode::{deserialize, serialize},
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Error,
    },
    gluesql_redb_storage::{REDB_STORAGE_FORMAT_VERSION, RedbStorage, migrate_to_latest},
    redb::{Database, ReadableTable, TableDefinition},
    std::fs::{create_dir, remove_dir_all, remove_file},
    uuid::Uuid,
};

const SCHEMA_TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("__SCHEMA__");
const META_TABLE: TableDefinition<&str, u32> = TableDefinition::new("__GLUESQL_META__");
const STORAGE_META_VERSION_KEY: &str = "storage_format_version";
const V2_FORMAT_VERSION: u32 = 2;

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

/// Creates a database in `GlueSQL` format v2 (redb internal file format v2).
fn write_v2_storage(path: &str, table_name: &str, rows: Vec<(Key, Vec<Value>)>) {
    let _ = create_dir("tmp");
    remove_path(path);

    // Database::create uses the default builder (file format v2).
    let db = Database::create(path).expect("create v2 database");
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
        let payload = serialize(&(&key, row)).expect("serialize v2 row");
        table
            .insert(table_key.as_slice(), payload)
            .expect("insert v2 row");
    }
    drop(table);

    // Write GlueSQL format version 2.
    let mut meta = txn.open_table(META_TABLE).expect("open metadata table");
    meta.insert(STORAGE_META_VERSION_KEY, &V2_FORMAT_VERSION)
        .expect("insert format version");
    drop(meta);

    txn.commit().expect("commit");
}

fn read_storage_format_version(path: &str) -> Option<u32> {
    let db = Database::open(path).expect("open database");
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

fn assert_redb_file_format_v3(path: &str) {
    let mut db = Database::open(path).expect("open database");
    let upgraded = db.upgrade().expect("check redb file format");

    assert!(!upgraded, "database should already use redb file format v3");
}

fn read_rows(path: &str, table_name: &str) -> Vec<(Key, Vec<Value>)> {
    let db = Database::open(path).expect("open database");
    let txn = db.begin_read().expect("begin read transaction");
    let table_def = TableDefinition::<&[u8], Vec<u8>>::new(table_name);
    let table = txn.open_table(table_def).expect("open data table");

    table
        .iter()
        .expect("iterate rows")
        .map(|entry| {
            let value = entry.expect("row entry").1.value();
            deserialize::<(Key, Vec<Value>)>(&value).expect("deserialize row")
        })
        .collect()
}

#[test]
fn v2_storage_requires_migration() {
    let path = test_path("redb-v2-requires-migration");
    write_v2_storage(&path, "Foo", vec![(Key::I64(1), vec![Value::I64(42)])]);

    let err = match RedbStorage::new(&path) {
        Ok(_storage) => panic!("migration should be required"),
        Err(err) => err,
    };
    assert_eq!(
        err,
        Error::StorageMsg(
            "[RedbStorage] migration required (found v2, expected v3); migrate redb-storage data to the latest format before opening"
                .to_owned(),
        ),
    );

    remove_path(&path);
}

#[test]
fn v2_to_v3_migration_keeps_rows_and_upgrades_format() {
    let path = test_path("redb-v2-to-v3");
    write_v2_storage(
        &path,
        "Foo",
        vec![
            (Key::I64(1), vec![Value::I64(10)]),
            (Key::I64(2), vec![Value::I64(20)]),
        ],
    );

    let report = migrate_to_latest(&path).expect("migrate v2 to v3");
    assert_eq!(report.migrated_tables, 0);
    assert_eq!(report.unchanged_tables, 1);
    assert_eq!(report.rewritten_rows, 0);
    assert_eq!(
        read_storage_format_version(&path),
        Some(REDB_STORAGE_FORMAT_VERSION),
    );
    assert_redb_file_format_v3(&path);

    // Rows are intact after migration.
    let rows = read_rows(&path, "Foo");
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .any(|(k, row)| k == &Key::I64(1) && row == &vec![Value::I64(10)])
    );
    assert!(
        rows.iter()
            .any(|(k, row)| k == &Key::I64(2) && row == &vec![Value::I64(20)])
    );

    // Storage opens normally after migration.
    RedbStorage::new(&path).expect("open v3 storage after migration");

    remove_path(&path);
}

#[test]
fn v2_to_v3_migration_is_idempotent() {
    let path = test_path("redb-v2-to-v3-idempotent");
    write_v2_storage(&path, "Foo", vec![]);

    let first = migrate_to_latest(&path).expect("first migration");
    assert_eq!(first.migrated_tables, 0);
    assert_eq!(first.unchanged_tables, 1);

    let second = migrate_to_latest(&path).expect("second migration");
    assert_eq!(second.migrated_tables, 0);
    assert_eq!(second.unchanged_tables, 1);
    assert_eq!(second.rewritten_rows, 0);

    RedbStorage::new(&path).expect("open after idempotent migration");

    remove_path(&path);
}
