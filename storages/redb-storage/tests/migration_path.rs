//! Filesystem input requirements of the public migration entry point.
#[path = "migration/database.rs"]
pub mod database;

use {database::TestFile, gluesql_core::error::Error, gluesql_redb_storage::migrate_to_latest};

#[test]
fn missing_path_is_rejected_without_creating_a_database() {
    let file = TestFile::empty();
    assert_eq!(
        migrate_to_latest(&file.0),
        Err(Error::StorageMsg(format!(
            "[RedbStorage] storage path '{}' does not exist",
            file.0.display()
        )))
    );
    assert!(!file.0.exists());
}

#[test]
fn directory_path_is_rejected() {
    let file = TestFile::empty();
    std::fs::create_dir(&file.0).unwrap();
    let result = migrate_to_latest(&file.0);
    std::fs::remove_dir(&file.0).unwrap();
    assert_eq!(
        result,
        Err(Error::StorageMsg(format!(
            "[RedbStorage] storage path '{}' is not a file",
            file.0.display()
        )))
    );
}
