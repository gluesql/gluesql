use {
    gluesql_core::prelude::{Error, Glue},
    gluesql_redb_storage::RedbStorage,
    std::fs::{create_dir, remove_file},
};

#[test]
fn reserved_table_name() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_reserved_table_name";
    let _ = remove_file(path);

    let storage = RedbStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    let result = glue.execute("CREATE TABLE __SCHEMA__ (id INTEGER);");

    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "cannot create table with reserved name: __SCHEMA__".to_owned(),
        ))
        .map(|payload| vec![payload])
    );

    let result = glue.execute("CREATE TABLE __GLUESQL_META__ (id INTEGER);");
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "cannot create table with reserved name: __GLUESQL_META__".to_owned(),
        ))
        .map(|payload| vec![payload])
    );

    let result = glue.execute("CREATE TABLE __GLUESQL_INDEX__ (id INTEGER)");
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "cannot create table with reserved name: __GLUESQL_INDEX__".to_owned(),
        ))
    );

    let result = glue.execute("CREATE TABLE __GLUESQL_INDEX__Items (id INTEGER)");
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "cannot create table with reserved name: __GLUESQL_INDEX__Items".to_owned(),
        ))
    );

    let result = glue.execute("CREATE TABLE \"a/b\" (id INTEGER)");
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "table name cannot contain '/': a/b".to_owned()
        ))
    );

    glue.execute("CREATE TABLE _GLUESQL_INDEX_ (id INTEGER)")
        .unwrap();

    let result = glue.execute("ALTER TABLE _GLUESQL_INDEX_ RENAME TO __GLUESQL_INDEX__Items");
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "cannot create table with reserved name: __GLUESQL_INDEX__Items".to_owned(),
        ))
    );

    let result = glue.execute("ALTER TABLE _GLUESQL_INDEX_ RENAME TO \"a/b\"");
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "table name cannot contain '/': a/b".to_owned()
        ))
    );
}
