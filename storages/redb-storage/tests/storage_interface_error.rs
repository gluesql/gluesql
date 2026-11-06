#[path = "migration/database.rs"]
pub mod database;

use {
    gluesql_core::{
        ast::{Expr, OrderByExpr},
        data::Schema,
        error::Error,
        store::{IndexMut, Store, StoreMut, Transaction},
    },
    gluesql_redb_storage::RedbStorage,
    std::fs::{create_dir, remove_file},
};

#[test]
fn redb_storage_interface_error() {
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

#[test]
fn index_creation_is_unsupported_by_the_storage_interface() {
    let file = database::TestFile::empty();
    let mut storage = RedbStorage::new(&file.0).unwrap();
    assert_eq!(
        storage.create_index(
            "Item",
            "by_id",
            &OrderByExpr {
                expr: Expr::Identifier("id".into()),
                asc: Some(true),
            }
        ),
        Err(Error::StorageMsg(
            "[Storage] Index::create_index is not supported".into()
        ))
    );
}

#[test]
fn store_methods_without_transaction_return_transaction_not_found() {
    let file = database::TestFile::empty();
    let path = &file.0;
    let mut storage = RedbStorage::new(path).expect("new storage");

    let err = storage
        .fetch_all_schemas()
        .expect_err("fetch_all_schemas should fail");
    assert_eq!(err, Error::StorageMsg("transaction not found".to_owned()));

    let schema = Schema::from_ddl("CREATE TABLE Foo (id INTEGER);").expect("parse schema");
    let err = storage
        .insert_schema(&schema)
        .expect_err("insert_schema should fail");
    assert_eq!(err, Error::StorageMsg("transaction not found".to_owned()));

    storage.rollback().expect("rollback without txn");
    storage.commit().expect("commit without txn");

    drop(storage);
}
