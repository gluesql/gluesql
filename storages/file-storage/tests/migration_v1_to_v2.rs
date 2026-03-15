use {
    gluesql_core::{
        data::{Key, Value},
        prelude::Glue,
        store::Store,
    },
    gluesql_file_storage::{FILE_STORAGE_FORMAT_VERSION, FileRow, FileStorage, migrate_to_latest},
    ron::ser::{PrettyConfig, to_string_pretty},
    serde::Serialize,
    std::{collections::BTreeMap, fs},
    uuid::Uuid,
};

const FORMAT_VERSION_PREFIX: &str = "-- gluesql:file-storage-format-version=";

#[derive(Serialize)]
enum V1DataRow {
    Vec(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

#[derive(Serialize)]
struct V1FileRow {
    key: Key,
    row: V1DataRow,
}

#[derive(Serialize)]
struct V1WrappedDataRow(Vec<Value>);

#[derive(Serialize)]
struct V1WrappedFileRow {
    key: Key,
    row: V1WrappedDataRow,
}

fn test_path(name: &str) -> String {
    format!("tmp/{name}-{}", Uuid::now_v7())
}

#[tokio::test]
async fn v2_create_table_writes_format_version_marker() {
    let path = test_path("format-marker");
    let storage = FileStorage::new(&path).expect("FileStorage::new");
    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Foo (id INTEGER);")
        .await
        .expect("create table");

    let schema = fs::read_to_string(format!("{path}/Foo.sql")).expect("read schema file");
    assert!(schema.starts_with(&format!(
        "-- gluesql:file-storage-format-version={FILE_STORAGE_FORMAT_VERSION}\n"
    )));

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn v1_schema_without_version_requires_migration() {
    let path = test_path("v1-requires-migration");
    fs::create_dir_all(&path).expect("create test path");
    fs::write(format!("{path}/Foo.sql"), "CREATE TABLE Foo (id INTEGER);").expect("write schema");

    let err = FileStorage::new(&path).expect_err("migration required");
    assert!(err.to_string().contains("migration required"));

    let report = migrate_to_latest(&path).expect("migrate to latest");
    assert_eq!(report.migrated_tables, 1);
    FileStorage::new(&path).expect("FileStorage::new after migration");

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn v1_to_v2_migration_updates_schema_and_rows() {
    let path = test_path("migrate-v1-v2");
    fs::create_dir_all(&path).expect("create test path");
    let storage = FileStorage::new(&path).expect("FileStorage::new");

    fs::write(format!("{path}/Foo.sql"), "CREATE TABLE Foo;").expect("write schema");
    fs::create_dir_all(storage.path("Foo")).expect("create table directory");

    let key = Key::I64(1);
    let v1_row = V1FileRow {
        key: key.clone(),
        row: V1DataRow::Map(BTreeMap::from([("id".to_owned(), Value::I64(7))])),
    };
    let row_data = to_string_pretty(&v1_row, PrettyConfig::default()).expect("serialize row");
    fs::write(storage.data_path("Foo", &key).expect("row path"), row_data).expect("write row");

    let key2 = Key::I64(2);
    let v1_row2 = V1FileRow {
        key: key2.clone(),
        row: V1DataRow::Vec(vec![Value::I64(10)]),
    };
    let row_data2 = to_string_pretty(&v1_row2, PrettyConfig::default()).expect("serialize row");
    fs::write(
        storage.data_path("Foo", &key2).expect("row path"),
        row_data2,
    )
    .expect("write row");

    let report = migrate_to_latest(&path).expect("migrate to latest");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.rewritten_rows, 2);

    let schema = fs::read_to_string(format!("{path}/Foo.sql")).expect("read schema");
    assert!(schema.starts_with(&format!(
        "-- gluesql:file-storage-format-version={FILE_STORAGE_FORMAT_VERSION}\n"
    )));

    let storage = FileStorage::new(&path).expect("FileStorage::new after migration");
    let row = storage
        .fetch_data("Foo", &key)
        .await
        .expect("fetch data")
        .expect("row exists");
    assert_eq!(
        row,
        vec![Value::Map(BTreeMap::from([(
            "id".to_owned(),
            Value::I64(7)
        )]))]
    );
    let row2 = storage
        .fetch_data("Foo", &key2)
        .await
        .expect("fetch data")
        .expect("row exists");
    assert_eq!(row2, vec![Value::I64(10)]);

    let second = migrate_to_latest(&path).expect("second migrate");
    assert_eq!(second.migrated_tables, 0);
    assert_eq!(second.unchanged_tables, 1);

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn newer_version_is_rejected() {
    let path = test_path("newer-version-rejected");
    fs::create_dir_all(&path).expect("create test path");
    fs::write(
        format!("{path}/Foo.sql"),
        format!("{FORMAT_VERSION_PREFIX}3\nCREATE TABLE Foo (id INTEGER);"),
    )
    .expect("write schema");

    let err = FileStorage::new(&path).expect_err("newer version should fail");
    assert!(err.to_string().contains("unsupported newer format version"));

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert!(err.to_string().contains("unsupported newer format version"));

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn unsupported_v0_version_is_rejected() {
    let path = test_path("unsupported-v0-version");
    fs::create_dir_all(&path).expect("create test path");
    fs::write(
        format!("{path}/Foo.sql"),
        format!("{FORMAT_VERSION_PREFIX}0\nCREATE TABLE Foo (id INTEGER);"),
    )
    .expect("write schema");

    let err = FileStorage::new(&path).expect_err("v0 version should fail");
    assert!(err.to_string().contains("unsupported format version v0"));

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert!(err.to_string().contains("unsupported format version v0"));

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn header_v1_version_is_rejected() {
    let path = test_path("header-v1-version-rejected");
    fs::create_dir_all(&path).expect("create test path");
    fs::write(
        format!("{path}/Foo.sql"),
        format!("{FORMAT_VERSION_PREFIX}1\nCREATE TABLE Foo (id INTEGER);"),
    )
    .expect("write schema");

    let err = FileStorage::new(&path).expect_err("v1 header should fail");
    assert!(err.to_string().contains("unsupported format version v1"));

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert!(err.to_string().contains("unsupported format version v1"));

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn migration_path_must_exist() {
    let path = test_path("migration-missing-path");
    let err = migrate_to_latest(&path).expect_err("missing path should fail");

    assert!(err.to_string().contains("does not exist"));
}

#[tokio::test]
async fn sql_extension_directory_is_ignored() {
    let path = test_path("sql-extension-directory");
    fs::create_dir_all(format!("{path}/fake.sql")).expect("create fake schema directory");

    FileStorage::new(&path).expect("FileStorage::new");
    let report = migrate_to_latest(&path).expect("migrate to latest");
    assert_eq!(report.migrated_tables, 0);
    assert_eq!(report.unchanged_tables, 0);
    assert_eq!(report.rewritten_rows, 0);

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn ron_extension_directory_is_ignored_during_row_migration() {
    let path = test_path("ron-extension-directory");
    fs::create_dir_all(&path).expect("create test path");
    let storage = FileStorage::new(&path).expect("FileStorage::new");
    fs::write(format!("{path}/Foo.sql"), "CREATE TABLE Foo;").expect("write v1 schema");

    let table_path = storage.path("Foo");
    fs::create_dir_all(&table_path).expect("create table directory");
    fs::create_dir_all(table_path.join("not-a-row.ron")).expect("create fake row directory");

    let report = migrate_to_latest(&path).expect("migrate to latest");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.rewritten_rows, 0);

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn v1_row_already_in_v2_shape_is_unchanged() {
    let path = test_path("v1-row-already-v2-shape");
    fs::create_dir_all(&path).expect("create test path");
    let storage = FileStorage::new(&path).expect("FileStorage::new");
    fs::write(format!("{path}/Foo.sql"), "CREATE TABLE Foo;").expect("write v1 schema");

    let table_path = storage.path("Foo");
    fs::create_dir_all(&table_path).expect("create table directory");

    let key = Key::I64(11);
    let row_path = storage.data_path("Foo", &key).expect("row path");
    let v2_row_data = to_string_pretty(
        &FileRow {
            key: key.clone(),
            row: vec![Value::I64(123)],
        },
        PrettyConfig::default(),
    )
    .expect("serialize v2 row");
    fs::write(&row_path, v2_row_data).expect("write v2 row");

    let report = migrate_to_latest(&path).expect("migrate to latest");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.rewritten_rows, 0);

    let storage = FileStorage::new(&path).expect("FileStorage::new after migration");
    let row = storage
        .fetch_data("Foo", &key)
        .await
        .expect("fetch data")
        .expect("row exists");
    assert_eq!(row, vec![Value::I64(123)]);

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn migration_path_must_be_directory() {
    let path = test_path("migration-not-directory");
    fs::write(&path, "not a directory").expect("write plain file");

    let err = migrate_to_latest(&path).expect_err("non-directory path should fail");
    assert!(err.to_string().contains("is not a directory"));

    let _ = fs::remove_file(&path);
}

#[tokio::test]
async fn malformed_schema_header_is_rejected() {
    let path = test_path("malformed-schema-header");
    fs::create_dir_all(&path).expect("create test path");
    fs::write(
        format!("{path}/Foo.sql"),
        format!("{FORMAT_VERSION_PREFIX}2"),
    )
    .expect("write schema");

    let err = FileStorage::new(&path).expect_err("missing ddl should fail");
    assert!(
        err.to_string()
            .contains("invalid schema format header: missing DDL after version marker")
    );

    fs::write(
        format!("{path}/Foo.sql"),
        format!("{FORMAT_VERSION_PREFIX}abc\nCREATE TABLE Foo (id INTEGER);"),
    )
    .expect("write schema");
    let err = FileStorage::new(&path).expect_err("invalid version should fail");
    assert!(err.to_string().contains("invalid digit found in string"));

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn v1_wrapped_row_is_migrated() {
    let path = test_path("migrate-v1-wrapped");
    fs::create_dir_all(&path).expect("create test path");
    let storage = FileStorage::new(&path).expect("FileStorage::new");
    fs::write(format!("{path}/Foo.sql"), "CREATE TABLE Foo;").expect("write schema");
    fs::create_dir_all(storage.path("Foo")).expect("create table directory");

    let key = Key::I64(7);
    let v1_row = V1WrappedFileRow {
        key: key.clone(),
        row: V1WrappedDataRow(vec![Value::I64(99), Value::Str("wrapped".to_owned())]),
    };
    let row_data = to_string_pretty(&v1_row, PrettyConfig::default()).expect("serialize row");
    fs::write(storage.data_path("Foo", &key).expect("row path"), row_data).expect("write row");

    let report = migrate_to_latest(&path).expect("migrate to latest");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.rewritten_rows, 1);

    let storage = FileStorage::new(&path).expect("FileStorage::new after migration");
    let row = storage
        .fetch_data("Foo", &key)
        .await
        .expect("fetch data")
        .expect("row exists");
    assert_eq!(row, vec![Value::I64(99), Value::Str("wrapped".to_owned())]);

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn v1_invalid_row_file_returns_error() {
    let path = test_path("migrate-v1-invalid-row");
    fs::create_dir_all(&path).expect("create test path");
    let storage = FileStorage::new(&path).expect("FileStorage::new");
    fs::write(format!("{path}/Foo.sql"), "CREATE TABLE Foo;").expect("write schema");
    fs::create_dir_all(storage.path("Foo")).expect("create table directory");

    let key = Key::I64(1);
    fs::write(
        storage.data_path("Foo", &key).expect("row path"),
        "this is not ron",
    )
    .expect("write invalid row");

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert!(err.to_string().contains("failed to parse v1 row file"));

    let _ = fs::remove_dir_all(&path);
}

#[tokio::test]
async fn migration_report_counts_migrated_and_unchanged_tables() {
    let path = test_path("migration-report-counts");
    fs::create_dir_all(&path).expect("create test path");
    let storage = FileStorage::new(&path).expect("FileStorage::new");

    fs::write(format!("{path}/Foo.sql"), "CREATE TABLE Foo;").expect("write v1 schema");
    fs::create_dir_all(storage.path("Foo")).expect("create table directory");
    let key = Key::I64(1);
    let v1_row = V1FileRow {
        key: key.clone(),
        row: V1DataRow::Vec(vec![Value::I64(1)]),
    };
    let row_data = to_string_pretty(&v1_row, PrettyConfig::default()).expect("serialize row");
    fs::write(storage.data_path("Foo", &key).expect("row path"), row_data).expect("write row");

    fs::write(
        format!("{path}/Bar.sql"),
        format!(
            "{FORMAT_VERSION_PREFIX}{FILE_STORAGE_FORMAT_VERSION}\nCREATE TABLE Bar (id INTEGER);"
        ),
    )
    .expect("write current schema");

    let report = migrate_to_latest(&path).expect("migrate to latest");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.unchanged_tables, 1);
    assert_eq!(report.rewritten_rows, 1);

    let _ = fs::remove_dir_all(&path);
}
