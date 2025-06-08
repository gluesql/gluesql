use {
    gluesql_core::prelude::Glue,
    gluesql_redb_storage::{RedbStorage, StorageError},
    redb::{Database, TableDefinition, TransactionError},
    std::fs::{create_dir, remove_file},
};

#[tokio::test]
async fn redb_transaction_error_conversion() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_transaction_error";
    let _ = remove_file(path);

    // Initialize database and create a table via RedbStorage
    let storage = RedbStorage::new(path).expect("create storage");
    let mut glue = Glue::new(storage);
    glue.execute("CREATE TABLE item (id INT)")
        .await
        .expect("create table");
    drop(glue); // release RedbStorage before using raw redb

    let db = Database::open(path).expect("open database");

    const SCHEMA_TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("__SCHEMA__");
    let tx = db.begin_read().expect("begin read");
    let table = tx.open_table(SCHEMA_TABLE).expect("open schema table");

    // Attempt to close while table still holds a reference
    let result = tx.close();
    let err = result.expect_err("close should fail while table live");
    let storage_err: StorageError = err.into();

    match storage_err {
        StorageError::RedbTransaction(e) => {
            assert!(matches!(*e, TransactionError::ReadTransactionStillInUse(_)))
        }
        other => panic!("unexpected error: {other:?}"),
    }

    drop(table);
}
