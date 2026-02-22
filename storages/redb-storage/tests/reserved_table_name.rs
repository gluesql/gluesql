use {
    gluesql_core::prelude::{Error, Glue},
    gluesql_redb_storage::RedbStorage,
    std::fs::{create_dir, remove_file},
};

#[tokio::test]
async fn reserved_table_name() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_reserved_table_name";
    let _ = remove_file(path);

    let storage = RedbStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    let result = glue.execute("CREATE TABLE __SCHEMA__ (id INTEGER);").await;

    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "cannot create table with reserved name: __SCHEMA__".to_owned(),
        ))
        .map(|payload| vec![payload])
    );

    let result = glue
        .execute("CREATE TABLE __GLUESQL_META__ (id INTEGER);")
        .await;
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "cannot create table with reserved name: __GLUESQL_META__".to_owned(),
        ))
        .map(|payload| vec![payload])
    );
}
