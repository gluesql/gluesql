use {
    gluesql_core::prelude::{Error, Glue},
    gluesql_redb_storage::RedbStorage,
};

#[tokio::test]
async fn redb_nested_transaction() {
    let _ = std::fs::create_dir("tmp");
    let path = "tmp/redb_nested_transaction";
    let _ = std::fs::remove_file(path);

    let storage = RedbStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    glue.execute("BEGIN").await.unwrap();
    let result = glue.execute("BEGIN").await;
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "nested transaction is not supported".to_owned()
        ))
        .map(|payload| vec![payload])
    );
    glue.execute("COMMIT;").await.unwrap();
}
