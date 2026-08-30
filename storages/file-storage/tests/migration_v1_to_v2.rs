use {
    gluesql_core::{
        data::{Key, Value},
        prelude::Glue,
        store::Store,
    },
    gluesql_file_storage::{FILE_STORAGE_FORMAT_VERSION, FileRow, FileStorage, migrate_to_latest},
    ron::ser::{PrettyConfig, to_string_pretty},
    serde::Serialize,
    std::{collections::BTreeMap, fs, path::Path, thread},
    uuid::Uuid,
};

const FORMAT_VERSION_PREFIX: &str = "-- gluesql:file-storage-format-version=";
const LOCK_SUFFIX: &str = ".migration-lock";
const STAGING_SUFFIX: &str = ".migrating";
const BACKUP_SUFFIX: &str = ".backup";

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

fn sibling(path: &str, suffix: &str) -> String {
    format!("{path}{suffix}")
}

fn cleanup(path: &str) {
    let _ = fs::remove_dir_all(path);
    let _ = fs::remove_dir_all(sibling(path, STAGING_SUFFIX));
    let _ = fs::remove_dir_all(sibling(path, BACKUP_SUFFIX));
    let _ = fs::remove_file(sibling(path, LOCK_SUFFIX));
}

/// Writes the lock a crashed migration would have left: a phase plus the
/// sibling directories that migration had claimed.
fn write_lock(path: &str, phase: &str, owns_staging: bool, owns_backup: bool) {
    let staging = if owns_staging { "owns-staging" } else { "" };
    let backup = if owns_backup { "owns-backup" } else { "" };

    fs::write(
        sibling(path, LOCK_SUFFIX),
        format!("{phase}\n{staging}\n{backup}\n"),
    )
    .expect("write lock");
}

fn snapshot(root: &str) -> BTreeMap<String, String> {
    fn collect(root: &Path, dir: &Path, files: &mut BTreeMap<String, String>) {
        for entry in fs::read_dir(dir).expect("read dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .expect("relative path")
                .to_string_lossy()
                .into_owned();

            if entry.file_type().expect("file type").is_dir() {
                files.insert(format!("{relative}/"), String::new());
                collect(root, &path, files);
                continue;
            }

            files.insert(relative, fs::read_to_string(&path).expect("read file"));
        }
    }

    let mut files = BTreeMap::new();
    collect(Path::new(root), Path::new(root), &mut files);

    files
}

fn v1_vec_row(key: &Key, values: Vec<Value>) -> String {
    let row = V1FileRow {
        key: key.clone(),
        row: V1DataRow::Vec(values),
    };

    to_string_pretty(&row, PrettyConfig::default()).expect("serialize v1 row")
}

fn v2_row(key: &Key, values: Vec<Value>) -> String {
    let row = FileRow {
        key: key.clone(),
        row: values,
    };

    to_string_pretty(&row, PrettyConfig::default()).expect("serialize v2 row")
}

/// Creates a v1 `Foo` table holding a valid row and, optionally, a broken one
/// that sorts after it.
fn write_v1_foo(path: &str, with_invalid_row: bool) -> FileStorage {
    fs::create_dir_all(path).expect("create test path");
    let storage = FileStorage::new(path).expect("FileStorage::new");
    fs::write(format!("{path}/Foo.sql"), "CREATE TABLE Foo;").expect("write v1 schema");
    fs::create_dir_all(storage.path("Foo")).expect("create table directory");

    let key = Key::I64(1);
    fs::write(
        storage.data_path("Foo", &key).expect("row path"),
        v1_vec_row(&key, vec![Value::I64(1)]),
    )
    .expect("write v1 row");

    if with_invalid_row {
        let broken = Key::I64(2);
        fs::write(
            storage.data_path("Foo", &broken).expect("row path"),
            "this is not ron",
        )
        .expect("write invalid row");
    }

    storage
}

fn assert_no_migration_artifacts(path: &str) {
    for suffix in [LOCK_SUFFIX, STAGING_SUFFIX, BACKUP_SUFFIX] {
        let artifact = sibling(path, suffix);
        assert!(
            !Path::new(&artifact).exists(),
            "leftover migration artifact: {artifact}"
        );
    }
}

#[test]
fn v2_create_table_writes_format_version_marker() {
    let path = test_path("format-marker");
    let storage = FileStorage::new(&path).expect("FileStorage::new");
    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Foo (id INTEGER);")
        .expect("create table");

    let schema = fs::read_to_string(format!("{path}/Foo.sql")).expect("read schema file");
    assert!(schema.starts_with(&format!(
        "-- gluesql:file-storage-format-version={FILE_STORAGE_FORMAT_VERSION}\n"
    )));

    let _ = fs::remove_dir_all(&path);
}

#[test]
fn v1_schema_without_version_requires_migration() {
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

#[test]
fn v1_to_v2_migration_updates_schema_and_rows() {
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
        .expect("fetch data")
        .expect("row exists");
    assert_eq!(row2, vec![Value::I64(10)]);

    let second = migrate_to_latest(&path).expect("second migrate");
    assert_eq!(second.migrated_tables, 0);
    assert_eq!(second.unchanged_tables, 1);

    let _ = fs::remove_dir_all(&path);
}

#[test]
fn newer_version_is_rejected() {
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

#[test]
fn unsupported_v0_version_is_rejected() {
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

#[test]
fn header_v1_version_is_rejected() {
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

#[test]
fn migration_path_must_exist() {
    let path = test_path("migration-missing-path");
    let err = migrate_to_latest(&path).expect_err("missing path should fail");

    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn sql_extension_directory_is_ignored() {
    let path = test_path("sql-extension-directory");
    fs::create_dir_all(format!("{path}/fake.sql")).expect("create fake schema directory");

    FileStorage::new(&path).expect("FileStorage::new");
    let report = migrate_to_latest(&path).expect("migrate to latest");
    assert_eq!(report.migrated_tables, 0);
    assert_eq!(report.unchanged_tables, 0);
    assert_eq!(report.rewritten_rows, 0);

    let _ = fs::remove_dir_all(&path);
}

#[test]
fn ron_extension_directory_is_ignored_during_row_migration() {
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

#[test]
fn v1_row_already_in_v2_shape_is_unchanged() {
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
        .expect("fetch data")
        .expect("row exists");
    assert_eq!(row, vec![Value::I64(123)]);

    let _ = fs::remove_dir_all(&path);
}

#[test]
fn migration_path_must_be_directory() {
    let path = test_path("migration-not-directory");
    fs::write(&path, "not a directory").expect("write plain file");

    let err = migrate_to_latest(&path).expect_err("non-directory path should fail");
    assert!(err.to_string().contains("is not a directory"));

    let _ = fs::remove_file(&path);
}

#[test]
fn malformed_schema_header_is_rejected() {
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

#[test]
fn v1_wrapped_row_is_migrated() {
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
        .expect("fetch data")
        .expect("row exists");
    assert_eq!(row, vec![Value::I64(99), Value::Str("wrapped".to_owned())]);

    let _ = fs::remove_dir_all(&path);
}

#[test]
fn v1_invalid_row_file_returns_error() {
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

#[test]
fn migration_report_counts_migrated_and_unchanged_tables() {
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

#[test]
fn build_failure_leaves_every_source_byte_untouched() {
    let path = test_path("build-failure-preserves-source");
    write_v1_foo(&path, true);

    let before = snapshot(&path);
    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert!(err.to_string().contains("failed to parse v1 row file"));

    assert_eq!(snapshot(&path), before);
    assert_no_migration_artifacts(&path);

    cleanup(&path);
}

#[test]
fn unexpected_source_entry_fails_without_deleting_data() {
    let path = test_path("unexpected-source-entry");
    let storage = write_v1_foo(&path, false);
    // A leftover from an interrupted `write_file_atomically`: the canonical row
    // file may be the one that is missing, so migration must not drop it.
    fs::write(
        storage.path("Foo").join("00010000000000000009.ron.bak-1"),
        "leftover",
    )
    .expect("write leftover file");

    let before = snapshot(&path);
    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert!(err.to_string().contains("unexpected entry"));

    assert_eq!(snapshot(&path), before);
    assert_no_migration_artifacts(&path);

    cleanup(&path);
}

#[test]
fn open_is_rejected_before_creating_canonical_path() {
    let path = test_path("open-rejected-while-locked");
    let _ = fs::create_dir_all("tmp");
    write_lock(&path, "building", false, false);

    let err = FileStorage::new(&path).expect_err("locked storage should not open");
    assert!(
        err.to_string()
            .contains("migration or recovery in progress")
    );
    assert!(
        !Path::new(&path).exists(),
        "canonical path must not be created while the lock exists"
    );

    cleanup(&path);
}

#[test]
fn a_staging_directory_beside_an_intact_storage_is_never_discarded() {
    let path = test_path("staging-beside-intact-storage");
    write_v1_foo(&path, false);

    let staging = sibling(&path, STAGING_SUFFIX);
    fs::create_dir_all(&staging).expect("create staging");
    fs::write(
        format!("{staging}/in-progress"),
        "owned by another migration",
    )
    .expect("write staging file");
    write_lock(&path, "building", true, false);

    let before = snapshot(&path);
    let err = migrate_to_latest(&path).expect_err("must refuse rather than guess");
    let err = err.to_string();
    assert!(err.contains("interrupted migration"));
    // The remedy has to be spelled out, because it is now a manual step.
    assert!(err.contains(&staging) && err.contains(&sibling(&path, LOCK_SUFFIX)));

    assert_eq!(snapshot(&path), before);
    assert!(
        Path::new(&format!("{staging}/in-progress")).exists(),
        "a staging directory that may still be in use must never be discarded"
    );

    cleanup(&path);
}

#[test]
fn removing_the_leftovers_lets_the_migration_run_again() {
    let path = test_path("leftovers-removed-then-retry");
    write_v1_foo(&path, false);

    let staging = sibling(&path, STAGING_SUFFIX);
    fs::create_dir_all(&staging).expect("create staging");
    write_lock(&path, "building", true, false);
    migrate_to_latest(&path).expect_err("refused while the leftovers are there");

    // The documented recovery for that refusal.
    fs::remove_dir_all(&staging).expect("remove staging");
    fs::remove_file(sibling(&path, LOCK_SUFFIX)).expect("remove lock");

    let report = migrate_to_latest(&path).expect("migration runs after cleanup");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.rewritten_rows, 1);
    assert_no_migration_artifacts(&path);

    cleanup(&path);
}

#[test]
fn interruption_after_first_rename_rolls_forward_completed_staging() {
    let path = test_path("roll-forward-completed-staging");
    write_v1_foo(&path, false);
    migrate_to_latest(&path).expect("prepare migrated storage");

    let migrated = snapshot(&path);
    let staging = sibling(&path, STAGING_SUFFIX);
    fs::rename(&path, &staging).expect("simulate completed staging");

    let backup = sibling(&path, BACKUP_SUFFIX);
    write_v1_foo(&backup, false);
    write_lock(&path, "staged", true, true);

    let report = migrate_to_latest(&path).expect("recover after first rename");
    assert_eq!(report.migrated_tables, 0);
    assert_eq!(report.unchanged_tables, 1);

    assert_eq!(snapshot(&path), migrated);
    assert_no_migration_artifacts(&path);

    cleanup(&path);
}

#[test]
fn interruption_after_first_rename_restores_backup_when_staging_is_incomplete() {
    let path = test_path("restore-backup-incomplete-staging");
    let backup = sibling(&path, BACKUP_SUFFIX);
    write_v1_foo(&backup, false);
    let original = snapshot(&backup);

    let staging = sibling(&path, STAGING_SUFFIX);
    fs::create_dir_all(&staging).expect("create staging");
    fs::write(format!("{staging}/Foo.sql"), "half written").expect("write partial staging");
    write_lock(&path, "building", true, true);

    let report = migrate_to_latest(&path).expect("recover with backup restore");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.rewritten_rows, 1);
    assert_no_migration_artifacts(&path);

    // The restored backup is the original storage, migrated from scratch, so
    // the half-written staging content must not be visible.
    assert_eq!(original.len(), 3);
    let schema = fs::read_to_string(format!("{path}/Foo.sql")).expect("read schema");
    assert!(schema.starts_with(&format!(
        "{FORMAT_VERSION_PREFIX}{FILE_STORAGE_FORMAT_VERSION}\n"
    )));
    let storage = FileStorage::new(&path).expect("open recovered storage");
    assert_eq!(
        storage
            .fetch_data("Foo", &Key::I64(1))
            .expect("fetch data")
            .expect("row exists"),
        vec![Value::I64(1)]
    );

    cleanup(&path);
}

#[test]
fn interruption_after_second_rename_removes_backup_and_lock() {
    let path = test_path("cleanup-after-second-rename");
    write_v1_foo(&path, false);
    migrate_to_latest(&path).expect("prepare migrated storage");
    let migrated = snapshot(&path);

    let backup = sibling(&path, BACKUP_SUFFIX);
    write_v1_foo(&backup, false);
    write_lock(&path, "staged", true, true);

    let report = migrate_to_latest(&path).expect("recover after second rename");
    assert_eq!(report.unchanged_tables, 1);
    assert_eq!(snapshot(&path), migrated);
    assert_no_migration_artifacts(&path);

    cleanup(&path);
}

#[test]
fn leftover_lock_alone_is_verified_and_removed() {
    let path = test_path("leftover-lock-only");
    write_v1_foo(&path, false);
    migrate_to_latest(&path).expect("prepare migrated storage");
    let migrated = snapshot(&path);

    write_lock(&path, "staged", true, true);
    let err = FileStorage::new(&path).expect_err("locked storage should not open");
    assert!(
        err.to_string()
            .contains("migration or recovery in progress")
    );

    let report = migrate_to_latest(&path).expect("recover leftover lock");
    assert_eq!(report.migrated_tables, 0);
    assert_eq!(report.unchanged_tables, 1);
    assert_eq!(snapshot(&path), migrated);
    assert_no_migration_artifacts(&path);

    FileStorage::new(&path).expect("open storage after lock cleanup");

    cleanup(&path);
}

#[test]
fn contradictory_layout_is_rejected_without_deleting_anything() {
    let path = test_path("contradictory-layout");
    write_v1_foo(&path, false);

    let staging = sibling(&path, STAGING_SUFFIX);
    let backup = sibling(&path, BACKUP_SUFFIX);
    fs::create_dir_all(&staging).expect("create staging");
    fs::write(format!("{staging}/marker"), "staging").expect("write staging marker");
    fs::create_dir_all(&backup).expect("create backup");
    fs::write(format!("{backup}/marker"), "backup").expect("write backup marker");
    write_lock(&path, "building", true, true);

    let before = snapshot(&path);
    let err = migrate_to_latest(&path).expect_err("contradictory layout should fail");
    assert!(err.to_string().contains("interrupted migration"));

    assert_eq!(snapshot(&path), before);
    assert!(Path::new(&format!("{staging}/marker")).exists());
    assert!(Path::new(&format!("{backup}/marker")).exists());
    assert!(Path::new(&sibling(&path, LOCK_SUFFIX)).exists());

    cleanup(&path);
}

#[test]
fn mixed_v1_and_v2_tables_survive_staging() {
    let path = test_path("mixed-v1-v2-staging");
    fs::create_dir_all(&path).expect("create test path");
    let storage = FileStorage::new(&path).expect("FileStorage::new");

    fs::write(format!("{path}/Foo.sql"), "CREATE TABLE Foo;").expect("write v1 schema");
    fs::create_dir_all(storage.path("Foo")).expect("create Foo directory");
    let v1_key = Key::I64(1);
    fs::write(
        storage.data_path("Foo", &v1_key).expect("row path"),
        v1_vec_row(&v1_key, vec![Value::I64(1)]),
    )
    .expect("write v1 row");
    // A v1 table may already hold rows written in the v2 shape.
    let v2_shaped_key = Key::I64(2);
    fs::write(
        storage.data_path("Foo", &v2_shaped_key).expect("row path"),
        v2_row(&v2_shaped_key, vec![Value::I64(2)]),
    )
    .expect("write v2-shaped row");

    fs::write(
        format!("{path}/Bar.sql"),
        format!(
            "{FORMAT_VERSION_PREFIX}{FILE_STORAGE_FORMAT_VERSION}\nCREATE TABLE Bar (id INTEGER);"
        ),
    )
    .expect("write v2 schema");
    fs::create_dir_all(storage.path("Bar")).expect("create Bar directory");
    let bar_key = Key::I64(3);
    fs::write(
        storage.data_path("Bar", &bar_key).expect("row path"),
        v2_row(&bar_key, vec![Value::I64(3)]),
    )
    .expect("write v2 row");

    let report = migrate_to_latest(&path).expect("migrate mixed storage");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.unchanged_tables, 1);
    assert_eq!(report.rewritten_rows, 1);
    assert_no_migration_artifacts(&path);

    let storage = FileStorage::new(&path).expect("open migrated storage");
    for (table, key, value) in [
        ("Foo", v1_key, Value::I64(1)),
        ("Foo", v2_shaped_key, Value::I64(2)),
        ("Bar", bar_key, Value::I64(3)),
    ] {
        assert_eq!(
            storage
                .fetch_data(table, &key)
                .expect("fetch data")
                .expect("row exists"),
            vec![value]
        );
    }

    let second = migrate_to_latest(&path).expect("re-run migration");
    assert_eq!(second.migrated_tables, 0);
    assert_eq!(second.unchanged_tables, 2);
    assert_eq!(second.rewritten_rows, 0);
    assert_no_migration_artifacts(&path);

    cleanup(&path);
}

#[test]
fn unsupported_version_in_any_table_fails_before_writing() {
    let path = test_path("unsupported-version-blocks-build");
    fs::create_dir_all(&path).expect("create test path");
    let storage = FileStorage::new(&path).expect("FileStorage::new");

    // The v1 table sorts first, so migration must still validate every schema
    // before it starts converting anything.
    fs::write(format!("{path}/Aaa.sql"), "CREATE TABLE Aaa;").expect("write v1 schema");
    fs::create_dir_all(storage.path("Aaa")).expect("create table directory");
    let key = Key::I64(1);
    fs::write(
        storage.data_path("Aaa", &key).expect("row path"),
        v1_vec_row(&key, vec![Value::I64(1)]),
    )
    .expect("write v1 row");
    fs::write(
        format!("{path}/Zzz.sql"),
        format!("{FORMAT_VERSION_PREFIX}3\nCREATE TABLE Zzz (id INTEGER);"),
    )
    .expect("write v3 schema");

    let before = snapshot(&path);
    let err = migrate_to_latest(&path).expect_err("unsupported version should fail");
    assert!(err.to_string().contains("unsupported newer format version"));

    assert_eq!(snapshot(&path), before);
    assert_no_migration_artifacts(&path);

    cleanup(&path);
}

#[test]
fn unrelated_sibling_directories_are_never_deleted() {
    let path = test_path("unrelated-siblings");
    write_v1_foo(&path, false);

    // A user directory that merely happens to sit at the backup path. Without a
    // migration lock it is not a migration artifact and must survive.
    let backup = sibling(&path, BACKUP_SUFFIX);
    fs::create_dir_all(&backup).expect("create unrelated directory");
    fs::write(format!("{backup}/mine.txt"), "not a migration backup").expect("write user file");

    let before = snapshot(&path);
    let err = migrate_to_latest(&path).expect_err("occupied sibling path should fail");
    assert!(err.to_string().contains("already exists"));

    assert_eq!(snapshot(&path), before);
    assert_eq!(
        fs::read_to_string(format!("{backup}/mine.txt")).expect("read user file"),
        "not a migration backup"
    );
    assert!(!Path::new(&sibling(&path, LOCK_SUFFIX)).exists());

    cleanup(&path);
}

#[test]
fn foreign_entries_are_copied_verbatim() {
    let path = test_path("foreign-entries-preserved");
    let storage = write_v1_foo(&path, false);

    // git-storage keeps its repository inside the storage directory.
    fs::create_dir_all(format!("{path}/.git/objects/ab")).expect("create fake git directory");
    fs::write(format!("{path}/.git/HEAD"), "ref: refs/heads/main\n").expect("write git file");
    fs::write(format!("{path}/.git/objects/ab/cdef"), "object payload").expect("write git object");
    fs::write(format!("{path}/notes.txt"), "keep me").expect("write user file");
    fs::write(storage.path("Foo").join("README"), "table note").expect("write table note");

    let report = migrate_to_latest(&path).expect("migrate storage with foreign entries");
    assert_eq!(report.migrated_tables, 1);
    assert_eq!(report.rewritten_rows, 1);
    assert_no_migration_artifacts(&path);

    for (relative, expected) in [
        (".git/HEAD", "ref: refs/heads/main\n"),
        (".git/objects/ab/cdef", "object payload"),
        ("notes.txt", "keep me"),
        ("Foo/README", "table note"),
    ] {
        assert_eq!(
            fs::read_to_string(format!("{path}/{relative}")).expect("read preserved file"),
            expected,
            "{relative} must survive the cutover"
        );
    }

    let storage = FileStorage::new(&path).expect("open migrated storage");
    assert_eq!(
        storage
            .fetch_data("Foo", &Key::I64(1))
            .expect("fetch data")
            .expect("row exists"),
        vec![Value::I64(1)]
    );

    cleanup(&path);
}

#[test]
fn concurrent_migrations_never_corrupt_the_storage() {
    let path = test_path("concurrent-migrations");
    fs::create_dir_all(&path).expect("create test path");
    let storage = FileStorage::new(&path).expect("FileStorage::new");
    fs::write(format!("{path}/Foo.sql"), "CREATE TABLE Foo;").expect("write v1 schema");
    fs::create_dir_all(storage.path("Foo")).expect("create table directory");

    let keys = (0..64).map(Key::I64).collect::<Vec<_>>();
    for key in &keys {
        fs::write(
            storage.data_path("Foo", key).expect("row path"),
            v1_vec_row(key, vec![Value::I64(7)]),
        )
        .expect("write v1 row");
    }

    let results = thread::scope(|scope| {
        let handles = (0..4)
            .map(|_| scope.spawn(|| migrate_to_latest(&path)))
            .collect::<Vec<_>>();

        handles
            .into_iter()
            .map(|handle| handle.join().expect("thread join"))
            .collect::<Vec<_>>()
    });

    // Concurrent migration is not a supported mode, so losers are allowed to
    // fail in several ways; what must hold is that no row is ever lost and the
    // storage converges once the contention is over.
    drop(results);

    migrate_to_latest(&path).expect("a single migration converges afterwards");
    assert_no_migration_artifacts(&path);
    let storage = FileStorage::new(&path).expect("open migrated storage");
    for key in &keys {
        assert_eq!(
            storage
                .fetch_data("Foo", key)
                .expect("fetch data")
                .expect("row exists"),
            vec![Value::I64(7)]
        );
    }

    cleanup(&path);
}

#[cfg(unix)]
#[test]
fn symlinked_storage_root_is_rejected() {
    let real = test_path("symlink-target");
    write_v1_foo(&real, false);
    let before = snapshot(&real);

    let link = test_path("symlink-root");
    std::os::unix::fs::symlink(
        Path::new(&real)
            .canonicalize()
            .expect("canonicalize target"),
        &link,
    )
    .expect("create symlink");

    let err = migrate_to_latest(&link).expect_err("symlinked root should be rejected");
    assert!(err.to_string().contains("symbolic link"));
    assert_eq!(snapshot(&real), before);

    // The resolved path migrates normally.
    migrate_to_latest(&real).expect("migrate resolved path");

    let _ = fs::remove_file(&link);
    cleanup(&real);
}

#[test]
fn interrupted_write_leftover_is_detected_without_any_v1_table() {
    let path = test_path("leftover-without-v1-table");
    write_v1_foo(&path, false);
    migrate_to_latest(&path).expect("migrate to latest");

    // The canonical schema is gone and only the interrupted write's backup is
    // left: the table must not be silently treated as absent.
    fs::rename(
        format!("{path}/Foo.sql"),
        format!("{path}/Foo.sql.bak-0192f000-0000-7000-8000-000000000000"),
    )
    .expect("simulate interrupted schema write");

    let before = snapshot(&path);
    let err = migrate_to_latest(&path).expect_err("leftover should be reported");
    assert!(err.to_string().contains("interrupted write"));

    assert_eq!(snapshot(&path), before);
    assert_no_migration_artifacts(&path);

    cleanup(&path);
}
