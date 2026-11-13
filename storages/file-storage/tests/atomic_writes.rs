use {
    gluesql_core::{
        data::{Key, Schema, Value},
        store::{Store, StoreMut},
    },
    gluesql_file_storage::FileStorage,
    std::{ffi::OsStr, fs, path::PathBuf},
    uuid::Uuid,
};

fn test_path(name: &str) -> String {
    format!("tmp/{name}-{}", Uuid::now_v7())
}

fn stray_files(path: &str, table: &str) -> Vec<PathBuf> {
    fs::read_dir(format!("{path}/{table}"))
        .expect("read table dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|p| {
            let name = p.file_name().and_then(OsStr::to_str).unwrap_or_default();
            name.contains(".tmp-") || name.contains(".bak-")
        })
        .collect()
}

#[test]
fn insert_data_leaves_no_leftover_temp_or_backup_files() {
    let path = test_path("insert-no-leftover");
    let mut storage = FileStorage::new(&path).expect("FileStorage::new");
    let schema = Schema::from_ddl("CREATE TABLE Foo (id INTEGER);").expect("parse schema");
    storage.insert_schema(&schema).expect("insert schema");

    storage
        .insert_data(
            "Foo",
            vec![
                (Key::I64(1), vec![Value::I64(1)]),
                (Key::I64(2), vec![Value::I64(2)]),
            ],
        )
        .expect("insert data");

    let leftovers = stray_files(&path, "Foo");
    assert!(
        leftovers.is_empty(),
        "unexpected leftover files: {leftovers:?}"
    );

    let _ = fs::remove_dir_all(&path);
}

#[test]
fn append_data_leaves_no_leftover_temp_or_backup_files() {
    let path = test_path("append-no-leftover");
    let mut storage = FileStorage::new(&path).expect("FileStorage::new");
    let schema = Schema::from_ddl("CREATE TABLE Foo (id INTEGER);").expect("parse schema");
    storage.insert_schema(&schema).expect("insert schema");

    storage
        .append_data("Foo", vec![vec![Value::I64(1)], vec![Value::I64(2)]])
        .expect("append data");

    let rows = storage
        .scan_data("Foo")
        .expect("scan data")
        .collect::<Result<Vec<_>, _>>()
        .expect("rows readable");
    assert_eq!(rows.len(), 2);

    let leftovers = stray_files(&path, "Foo");
    assert!(
        leftovers.is_empty(),
        "unexpected leftover files: {leftovers:?}"
    );

    let _ = fs::remove_dir_all(&path);
}

#[test]
fn reinserting_existing_key_replaces_content_without_leftovers() {
    let path = test_path("insert-overwrite");
    let mut storage = FileStorage::new(&path).expect("FileStorage::new");
    let schema = Schema::from_ddl("CREATE TABLE Foo (id INTEGER);").expect("parse schema");
    storage.insert_schema(&schema).expect("insert schema");

    let key = Key::I64(1);
    storage
        .insert_data("Foo", vec![(key.clone(), vec![Value::I64(1)])])
        .expect("first insert");
    storage
        .insert_data("Foo", vec![(key.clone(), vec![Value::I64(99)])])
        .expect("second insert (overwrite)");

    let row = storage
        .fetch_data("Foo", &key)
        .expect("fetch data")
        .expect("row exists");
    assert_eq!(row, vec![Value::I64(99)]);

    let leftovers = stray_files(&path, "Foo");
    assert!(
        leftovers.is_empty(),
        "unexpected leftover files: {leftovers:?}"
    );

    let _ = fs::remove_dir_all(&path);
}

#[cfg(unix)]
#[test]
fn insert_into_readonly_table_dir_preserves_existing_row() {
    use std::os::unix::fs::PermissionsExt;

    let path = test_path("insert-readonly-dir");
    let mut storage = FileStorage::new(&path).expect("FileStorage::new");
    let schema = Schema::from_ddl("CREATE TABLE Foo (id INTEGER);").expect("parse schema");
    storage.insert_schema(&schema).expect("insert schema");

    let key = Key::I64(1);
    storage
        .insert_data("Foo", vec![(key.clone(), vec![Value::I64(1)])])
        .expect("first insert");

    let table_dir = storage.path("Foo");
    let original_perms = fs::metadata(&table_dir).expect("metadata").permissions();
    let mut readonly_perms = original_perms.clone();
    readonly_perms.set_mode(0o500); // r-x: can't create a new temp-file entry
    fs::set_permissions(&table_dir, readonly_perms).expect("set readonly");

    let result = storage.insert_data("Foo", vec![(key.clone(), vec![Value::I64(2)])]);

    fs::set_permissions(&table_dir, original_perms).expect("restore permissions");

    result.expect_err("insert into a read-only table directory should fail");

    let row = storage
        .fetch_data("Foo", &key)
        .expect("fetch data")
        .expect("row exists");
    assert_eq!(row, vec![Value::I64(1)]);

    let _ = fs::remove_dir_all(&path);
}
