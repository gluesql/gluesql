//! Storage initialization and version admission through both public constructors.
#[path = "migration/database.rs"]
pub mod database;

use {
    database::{META, SCHEMAS, Snapshot, TestFile, VERSION_KEY, snapshot, write_database},
    gluesql_core::error::Error,
    gluesql_redb_storage::{
        MigrationReport, REDB_STORAGE_FORMAT_VERSION, RedbStorage, migrate_to_latest,
    },
    redb::{Database, TableDefinition},
    std::collections::BTreeMap,
};

fn state(version: Option<u32>) -> Snapshot {
    Snapshot {
        version,
        schemas: BTreeMap::new(),
        rows: BTreeMap::new(),
    }
}

fn assert_both_constructors_reject(file: &TestFile, message: &str) {
    assert_eq!(
        RedbStorage::new(&file.0).err().unwrap(),
        Error::StorageMsg(message.into())
    );
    let db = Database::open(&file.0).unwrap();
    assert_eq!(
        RedbStorage::from_database(db).err().unwrap(),
        Error::StorageMsg(message.into())
    );
}

#[test]
fn new_storage_records_v4_and_the_current_physical_format() {
    assert_eq!(REDB_STORAGE_FORMAT_VERSION, 4);
    let file = TestFile::empty();
    drop(RedbStorage::new(&file.0).unwrap());
    assert_eq!(snapshot(&file).version, Some(4));
    let mut db = Database::open(&file.0).unwrap();
    assert!(!db.upgrade().unwrap());
    drop(RedbStorage::from_database(db).unwrap());
}

#[test]
fn both_constructors_block_each_supported_old_version_until_migration() {
    for (version, found, physical_v3) in [(None, 1, false), (Some(2), 2, false), (Some(3), 3, true)]
    {
        let file = TestFile::empty();
        write_database(&file, physical_v3, &state(version));
        let before = snapshot(&file);
        let message = format!(
            "[RedbStorage] migration required (found v{found}, expected v4); migrate redb-storage data to the latest format before opening"
        );
        assert_both_constructors_reject(&file, &message);
        assert_eq!(snapshot(&file), before);
        migrate_to_latest(&file.0).unwrap();
        assert_eq!(snapshot(&file).version, Some(4));
        drop(RedbStorage::new(&file.0).unwrap());
        drop(RedbStorage::from_database(Database::open(&file.0).unwrap()).unwrap());
    }
}

#[test]
fn unsupported_and_newer_versions_are_rejected_without_changes() {
    // Historical v1 is represented by absent metadata, not a stored value 1.
    for (version, message) in [
        (0, "[RedbStorage] unsupported format version v0"),
        (1, "[RedbStorage] unsupported format version v1"),
        (5, "[RedbStorage] unsupported newer format version v5"),
    ] {
        let file = TestFile::empty();
        write_database(&file, true, &state(Some(version)));
        let before = snapshot(&file);
        assert_both_constructors_reject(&file, message);
        assert_eq!(
            migrate_to_latest(&file.0),
            Err(Error::StorageMsg(message.into()))
        );
        assert_eq!(snapshot(&file), before);
    }
}

#[test]
fn metadata_table_without_a_version_key_is_not_treated_as_v1() {
    let file = TestFile::empty();
    write_database(&file, true, &state(Some(3)));
    {
        let db = Database::open(&file.0).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(META).unwrap().remove(VERSION_KEY).unwrap();
        txn.commit().unwrap();
    }
    let message = "[RedbStorage] invalid storage format metadata: missing format version key";
    assert_both_constructors_reject(&file, message);
    assert_eq!(
        migrate_to_latest(&file.0),
        Err(Error::StorageMsg(message.into()))
    );
}

#[test]
fn metadata_table_with_the_wrong_value_type_is_rejected() {
    let file = TestFile::empty();
    let db = Database::create(&file.0).unwrap();
    let txn = db.begin_write().unwrap();
    txn.open_table(TableDefinition::<&str, Vec<u8>>::new("__GLUESQL_META__"))
        .unwrap()
        .insert(VERSION_KEY, vec![2, 0, 0, 0])
        .unwrap();
    txn.commit().unwrap();
    let message = db
        .begin_read()
        .unwrap()
        .open_table(META)
        .unwrap_err()
        .to_string();
    drop(db);
    assert_both_constructors_reject(&file, &message);
    assert_eq!(migrate_to_latest(&file.0), Err(Error::StorageMsg(message)));
}

#[test]
fn empty_old_database_is_initialized_and_current_empty_database_is_unchanged() {
    let file = TestFile::empty();
    drop(Database::create(&file.0).unwrap());
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport::default()
    );
    let after = snapshot(&file);
    assert_eq!(after.version, Some(4));
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport::default()
    );
    assert_eq!(snapshot(&file), after);
    RedbStorage::new(&file.0).unwrap();
}

#[test]
fn current_empty_database_does_not_need_a_schema_table() {
    let file = TestFile::empty();
    drop(RedbStorage::new(&file.0).unwrap());
    let before = snapshot(&file);
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport::default()
    );
    assert_eq!(snapshot(&file), before);
}

#[test]
fn current_version_does_not_hide_a_schema_table_type_error() {
    let file = TestFile::empty();
    let db = Database::create(&file.0).unwrap();
    let txn = db.begin_write().unwrap();
    txn.open_table(META)
        .unwrap()
        .insert(VERSION_KEY, &4)
        .unwrap();
    txn.open_table(TableDefinition::<&[u8], Vec<u8>>::new("__SCHEMA__"))
        .unwrap()
        .insert(b"Foo".as_slice(), vec![1])
        .unwrap();
    txn.commit().unwrap();
    let message = db
        .begin_read()
        .unwrap()
        .open_table(SCHEMAS)
        .unwrap_err()
        .to_string();
    drop(db);
    assert_eq!(migrate_to_latest(&file.0), Err(Error::StorageMsg(message)));
}
