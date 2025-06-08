use gluesql_core::error::Error;
use gluesql_redb_storage::RedbStorage;
use std::fs::{create_dir, remove_file};

#[tokio::test]
async fn redb_storage_interface_error() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_storage_interface_error";
    let _ = remove_file(path);

    let storage1 = RedbStorage::new(path).expect("open first storage");

    // Attempt to open the same database again using the storage interface
    let result = RedbStorage::new(path);
    let err = result.err().expect("second open should fail");

    match err {
        Error::StorageMsg(msg) => assert!(msg.contains("Database already open")),
        other => panic!("unexpected error: {other:?}"),
    }

    drop(storage1);
}
