use {
    gluesql_core::{
        ast::ColumnDef,
        data::{Key, Schema, Value},
        error::Error,
        prelude::DataType,
    },
    gluesql_sled_storage::{
        MigrationReport, SLED_STORAGE_FORMAT_VERSION, SledStorage, migrate_to_latest,
    },
    serde::{Deserialize, Serialize},
    sled::Db,
    std::{
        collections::BTreeMap,
        fs::{remove_dir_all, remove_file, write},
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    },
};

const STORAGE_FORMAT_VERSION_KEY: &str = "__GLUESQL_STORAGE_FORMAT_VERSION__";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V1SnapshotItem<T> {
    data: T,
    created_by: u64,
    deleted_by: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V1Snapshot<T>(Vec<V1SnapshotItem<T>>);

impl<T> V1Snapshot<T> {
    fn new(txid: u64, data: T) -> Self {
        Self(vec![V1SnapshotItem {
            data,
            created_by: txid,
            deleted_by: None,
        }])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum V1DataRow {
    Vec(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V1VecDataRow(Vec<Value>);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V2SnapshotItem<T> {
    data: T,
    created_by: u64,
    deleted_by: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V2Snapshot<T>(Vec<V2SnapshotItem<T>>);

fn test_path(name: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();

    PathBuf::from(format!("./tmp/{name}-{suffix}"))
}

fn data_key(table_name: &str, key: Vec<u8>) -> Vec<u8> {
    format!("data/{table_name}/")
        .into_bytes()
        .into_iter()
        .chain(key)
        .collect()
}

fn write_schema_snapshot(tree: &Db, schema: Schema) {
    let schema_key = format!("schema/{}", schema.table_name);
    let snapshot = V1Snapshot::new(1, schema);
    let value = bincode::serialize(&snapshot).expect("serialize schema snapshot");
    tree.insert(schema_key, value)
        .expect("insert schema snapshot");
}

fn write_v1_data_snapshot<T: Serialize>(tree: &Db, table_name: &str, key: Vec<u8>, row: T) {
    let data_key = data_key(table_name, key);
    let snapshot = V1Snapshot::new(1, row);
    let value = bincode::serialize(&snapshot).expect("serialize row snapshot");
    tree.insert(data_key, value).expect("insert row snapshot");
}

fn write_v2_data_snapshot(tree: &Db, table_name: &str, key: Vec<u8>, row: Vec<Value>) {
    let data_key = data_key(table_name, key);
    let snapshot = V2Snapshot(vec![V2SnapshotItem {
        data: row,
        created_by: 1,
        deleted_by: None,
    }]);
    let value = bincode::serialize(&snapshot).expect("serialize row snapshot");
    tree.insert(data_key, value).expect("insert row snapshot");
}

fn write_storage_format_version(path: &Path, version_bytes: &[u8]) {
    let tree = sled::open(path).expect("open sled");
    tree.insert(STORAGE_FORMAT_VERSION_KEY, version_bytes)
        .expect("insert version");
    tree.flush().expect("flush version");
}

fn setup_v1_storage(path: &Path) -> BTreeMap<String, Value> {
    let _ = remove_dir_all(path);
    let tree = sled::open(path).expect("open sled");

    let user_schema = Schema {
        table_name: "User".to_owned(),
        column_defs: Some(vec![
            ColumnDef {
                name: "id".to_owned(),
                data_type: DataType::Int,
                nullable: false,
                default: None,
                unique: None,
                comment: None,
            },
            ColumnDef {
                name: "name".to_owned(),
                data_type: DataType::Text,
                nullable: false,
                default: None,
                unique: None,
                comment: None,
            },
        ]),
        indexes: Vec::new(),
        engine: None,
        foreign_keys: Vec::new(),
        comment: None,
    };
    write_schema_snapshot(&tree, user_schema);

    let logs_schema = Schema {
        table_name: "Logs".to_owned(),
        column_defs: None,
        indexes: Vec::new(),
        engine: None,
        foreign_keys: Vec::new(),
        comment: None,
    };
    write_schema_snapshot(&tree, logs_schema);

    write_v1_data_snapshot(
        &tree,
        "User",
        Key::I64(1).to_cmp_be_bytes().expect("user key"),
        V1DataRow::Vec(vec![Value::I64(1), Value::Str("Alice".to_owned())]),
    );

    let mut log_row = BTreeMap::new();
    log_row.insert("event".to_owned(), Value::Str("login".to_owned()));
    log_row.insert("ok".to_owned(), Value::Bool(true));
    write_v1_data_snapshot(
        &tree,
        "Logs",
        vec![0, 0, 0, 0, 0, 0, 0, 1],
        V1DataRow::Map(log_row.clone()),
    );

    tree.flush().expect("flush sled");
    drop(tree);

    log_row
}

fn read_storage_format_version(path: &Path) -> u32 {
    let tree = sled::open(path).expect("open sled");
    let value = tree
        .get(STORAGE_FORMAT_VERSION_KEY)
        .expect("read storage format version key")
        .expect("storage format version exists");
    let bytes: [u8; 4] = value.as_ref().try_into().expect("u32 bytes");

    u32::from_be_bytes(bytes)
}

fn read_row_snapshot(path: &Path, table_name: &str, key: Vec<u8>) -> Vec<Value> {
    let tree = sled::open(path).expect("open sled");
    let key = data_key(table_name, key);
    let value = tree
        .get(key)
        .expect("read row")
        .expect("row snapshot exists")
        .to_vec();
    let snapshot: V1Snapshot<Vec<Value>> =
        bincode::deserialize(&value).expect("deserialize migrated snapshot");

    snapshot.0.into_iter().next().expect("snapshot head").data
}

#[test]
fn opening_v1_storage_requires_migration() {
    let path = test_path("sled-migration-required");
    setup_v1_storage(&path);

    let actual = SledStorage::new(&path).map(|_| ());
    let expected = Err(Error::StorageMsg(
        "[SledStorage] migration required (found v1, expected v2); migrate sled-storage data to the latest format before opening".to_owned(),
    ));
    assert_eq!(actual, expected);

    remove_dir_all(path).expect("cleanup");
}

#[test]
fn migrate_v1_to_v2_rewrites_rows_and_sets_version() {
    let path = test_path("sled-migration-v1-to-v2");
    let expected_log_row = setup_v1_storage(&path);

    let report = migrate_to_latest(&path).expect("migrate v1 to v2");
    assert_eq!(
        report,
        MigrationReport {
            migrated_tables: 2,
            unchanged_tables: 0,
            rewritten_rows: 2,
        }
    );

    assert_eq!(
        read_storage_format_version(&path),
        SLED_STORAGE_FORMAT_VERSION
    );
    assert!(SledStorage::new(&path).is_ok());

    let user_row = read_row_snapshot(
        &path,
        "User",
        Key::I64(1).to_cmp_be_bytes().expect("user key"),
    );
    assert_eq!(
        user_row,
        vec![Value::I64(1), Value::Str("Alice".to_owned())]
    );

    let logs_row = read_row_snapshot(&path, "Logs", vec![0, 0, 0, 0, 0, 0, 0, 1]);
    assert_eq!(logs_row, vec![Value::Map(expected_log_row)]);

    let report = migrate_to_latest(&path).expect("idempotent migration");
    assert_eq!(
        report,
        MigrationReport {
            migrated_tables: 0,
            unchanged_tables: 2,
            rewritten_rows: 0,
        }
    );

    remove_dir_all(path).expect("cleanup");
}

#[test]
fn migrate_rejects_unsupported_newer_version() {
    let path = test_path("sled-migration-unsupported-newer");
    let _ = remove_dir_all(&path);

    write_storage_format_version(&path, &(SLED_STORAGE_FORMAT_VERSION + 1).to_be_bytes());

    let actual = migrate_to_latest(&path);
    let expected = Err(Error::StorageMsg(
        "[SledStorage] unsupported newer format version v3".to_owned(),
    ));
    assert_eq!(actual, expected);

    remove_dir_all(path).expect("cleanup");
}

#[test]
fn migrate_rejects_unsupported_older_version() {
    let path = test_path("sled-migration-unsupported-older");
    let _ = remove_dir_all(&path);

    write_storage_format_version(&path, &0_u32.to_be_bytes());

    let actual = migrate_to_latest(&path);
    let expected = Err(Error::StorageMsg(
        "[SledStorage] unsupported format version v0".to_owned(),
    ));
    assert_eq!(actual, expected);

    remove_dir_all(path).expect("cleanup");
}

#[test]
fn opening_storage_rejects_unsupported_versions() {
    let path = test_path("sled-open-unsupported-version");
    let _ = remove_dir_all(&path);

    write_storage_format_version(&path, &0_u32.to_be_bytes());
    let actual = SledStorage::new(&path).map(|_| ());
    let expected = Err(Error::StorageMsg(
        "[SledStorage] unsupported format version v0".to_owned(),
    ));
    assert_eq!(actual, expected);

    write_storage_format_version(&path, &(SLED_STORAGE_FORMAT_VERSION + 1).to_be_bytes());
    let actual = SledStorage::new(&path).map(|_| ());
    let expected = Err(Error::StorageMsg(
        "[SledStorage] unsupported newer format version v3".to_owned(),
    ));
    assert_eq!(actual, expected);

    remove_dir_all(path).expect("cleanup");
}

#[test]
fn migrate_rejects_invalid_storage_metadata_length() {
    let path = test_path("sled-migration-invalid-version-bytes");
    let _ = remove_dir_all(&path);

    write_storage_format_version(&path, &[1, 2, 3]);

    let actual = migrate_to_latest(&path);
    let expected = Err(Error::StorageMsg(
        "[SledStorage] invalid storage format metadata: expected 4 bytes, found 3".to_owned(),
    ));
    assert_eq!(actual, expected);

    remove_dir_all(path).expect("cleanup");
}

#[test]
fn migrate_rejects_nonexistent_path() {
    let path = test_path("sled-migration-missing-path");

    let actual = migrate_to_latest(&path);
    let expected = Err(Error::StorageMsg(format!(
        "[SledStorage] storage path '{}' does not exist",
        path.display()
    )));
    assert_eq!(actual, expected);
}

#[test]
fn migrate_rejects_file_path() {
    let path = test_path("sled-migration-file-path");
    let _ = remove_file(&path);
    write(&path, b"not a directory").expect("write file path");

    let actual = migrate_to_latest(&path);
    let expected = Err(Error::StorageMsg(format!(
        "[SledStorage] storage path '{}' is not a directory",
        path.display()
    )));
    assert_eq!(actual, expected);

    remove_file(path).expect("cleanup");
}

#[test]
fn migrate_v1_to_v2_supports_all_v1_snapshot_shapes() {
    let path = test_path("sled-migration-v1-shapes");
    let _ = remove_dir_all(&path);
    let tree = sled::open(&path).expect("open sled");

    write_schema_snapshot(
        &tree,
        Schema {
            table_name: "Mixed".to_owned(),
            column_defs: None,
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        },
    );

    let mut map_row = BTreeMap::new();
    map_row.insert("name".to_owned(), Value::Str("map".to_owned()));

    write_v1_data_snapshot(
        &tree,
        "Mixed",
        Key::I64(1).to_cmp_be_bytes().expect("key 1"),
        V1DataRow::Vec(vec![Value::I64(1)]),
    );
    write_v1_data_snapshot(
        &tree,
        "Mixed",
        Key::I64(2).to_cmp_be_bytes().expect("key 2"),
        V1DataRow::Map(map_row.clone()),
    );
    write_v1_data_snapshot(
        &tree,
        "Mixed",
        Key::I64(3).to_cmp_be_bytes().expect("key 3"),
        V1VecDataRow(vec![Value::I64(3)]),
    );
    write_v1_data_snapshot(
        &tree,
        "Mixed",
        Key::I64(4).to_cmp_be_bytes().expect("key 4"),
        vec![Value::I64(4)],
    );
    write_v2_data_snapshot(
        &tree,
        "Mixed",
        Key::I64(5).to_cmp_be_bytes().expect("key 5"),
        vec![Value::I64(5)],
    );

    tree.flush().expect("flush sled");
    drop(tree);

    let report = migrate_to_latest(&path).expect("migrate");
    assert_eq!(
        report,
        MigrationReport {
            migrated_tables: 1,
            unchanged_tables: 0,
            rewritten_rows: 5,
        }
    );

    assert_eq!(
        read_row_snapshot(
            &path,
            "Mixed",
            Key::I64(1).to_cmp_be_bytes().expect("key 1")
        ),
        vec![Value::I64(1)]
    );
    assert_eq!(
        read_row_snapshot(
            &path,
            "Mixed",
            Key::I64(2).to_cmp_be_bytes().expect("key 2")
        ),
        vec![Value::Map(map_row)]
    );
    assert_eq!(
        read_row_snapshot(
            &path,
            "Mixed",
            Key::I64(3).to_cmp_be_bytes().expect("key 3")
        ),
        vec![Value::I64(3)]
    );
    assert_eq!(
        read_row_snapshot(
            &path,
            "Mixed",
            Key::I64(4).to_cmp_be_bytes().expect("key 4")
        ),
        vec![Value::I64(4)]
    );
    assert_eq!(
        read_row_snapshot(
            &path,
            "Mixed",
            Key::I64(5).to_cmp_be_bytes().expect("key 5")
        ),
        vec![Value::I64(5)]
    );

    remove_dir_all(path).expect("cleanup");
}

#[test]
fn migrate_v1_storage_without_data_rows() {
    let path = test_path("sled-migration-without-data-rows");
    let _ = remove_dir_all(&path);
    let tree = sled::open(&path).expect("open sled");

    write_schema_snapshot(
        &tree,
        Schema {
            table_name: "OnlySchema".to_owned(),
            column_defs: Some(vec![ColumnDef {
                name: "id".to_owned(),
                data_type: DataType::Int,
                nullable: false,
                default: None,
                unique: None,
                comment: None,
            }]),
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        },
    );
    tree.flush().expect("flush sled");
    drop(tree);

    let report = migrate_to_latest(&path).expect("migrate");
    assert_eq!(
        report,
        MigrationReport {
            migrated_tables: 1,
            unchanged_tables: 0,
            rewritten_rows: 0,
        }
    );
    assert_eq!(
        read_storage_format_version(&path),
        SLED_STORAGE_FORMAT_VERSION
    );

    remove_dir_all(path).expect("cleanup");
}

#[test]
fn migrate_rejects_invalid_v1_row_snapshot_payload() {
    let path = test_path("sled-migration-invalid-v1-row-snapshot");
    let _ = remove_dir_all(&path);
    let tree = sled::open(&path).expect("open sled");
    write_schema_snapshot(
        &tree,
        Schema {
            table_name: "Broken".to_owned(),
            column_defs: None,
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        },
    );
    tree.insert(
        data_key("Broken", Key::I64(1).to_cmp_be_bytes().expect("key")),
        Vec::<u8>::new(),
    )
    .expect("insert invalid payload");
    tree.flush().expect("flush sled");
    drop(tree);

    let actual = migrate_to_latest(&path);
    let expected = Err(Error::StorageMsg(
        "[SledStorage] failed to parse v1 row snapshot during migration: io error: unexpected end of file".to_owned(),
    ));
    assert_eq!(actual, expected);

    remove_dir_all(path).expect("cleanup");
}

#[test]
fn migrate_v2_storage_with_invalid_schema_snapshot_is_rejected() {
    let path = test_path("sled-v2-invalid-schema-snapshot");
    let _ = remove_dir_all(&path);
    let tree = sled::open(&path).expect("open sled");

    tree.insert(
        STORAGE_FORMAT_VERSION_KEY,
        &SLED_STORAGE_FORMAT_VERSION.to_be_bytes(),
    )
    .expect("insert storage format version");
    tree.insert("schema/Broken", vec![1_u8, 2, 3, 4])
        .expect("insert invalid schema snapshot");
    tree.flush().expect("flush sled");
    drop(tree);

    let err = migrate_to_latest(&path).expect_err("migration should fail");
    assert!(matches!(err, Error::StorageMsg(_)));

    remove_dir_all(path).expect("cleanup");
}
