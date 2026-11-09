#[path = "migration/database.rs"]
pub mod database;

use {
    gluesql_core::{
        ast::{Expr, OrderByExpr},
        data::{Key, Schema, Value},
        error::Error,
        store::{IndexError, IndexMut, Store, StoreMut, Transaction},
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
fn index_errors_keep_the_shared_storage_interface_variant() {
    let file = database::TestFile::empty();
    let mut storage = RedbStorage::new(&file.0).unwrap();
    storage.begin(false).unwrap();
    assert_eq!(
        storage.create_index(
            "Item",
            "by_id",
            &OrderByExpr {
                expr: Expr::Identifier("id".into()),
                asc: Some(true),
            }
        ),
        Err(Error::Index(IndexError::TableNotFound("Item".into())))
    );
    storage.rollback().unwrap();
}

#[test]
fn store_methods_without_transaction_return_transaction_not_found() {
    let file = database::TestFile::empty();
    let path = &file.0;
    let mut storage = RedbStorage::new(path).expect("new storage");

    assert_eq!(storage.fetch_all_schemas(), Ok(Vec::new()));

    let schema = Schema::from_ddl("CREATE TABLE Foo (id INTEGER);").expect("parse schema");
    let err = storage
        .insert_schema(&schema)
        .expect_err("insert_schema should fail");
    assert_eq!(err, Error::StorageMsg("transaction not found".to_owned()));

    let expected = Err(Error::StorageMsg("transaction not found".to_owned()));
    assert_eq!(
        storage.append_data("Foo", vec![vec![Value::I64(1)]]),
        expected
    );
    assert_eq!(
        storage.insert_data("Foo", vec![(Key::I64(1), vec![Value::I64(1)])]),
        expected
    );
    assert_eq!(storage.delete_data("Foo", vec![Key::I64(1)]), expected);
    assert_eq!(storage.delete_schema("Foo"), expected);

    storage.rollback().expect("rollback without txn");
    storage.commit().expect("commit without txn");

    drop(storage);
}
