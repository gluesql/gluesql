use {
    futures::executor::block_on,
    gluesql_composite_storage::CompositeStorage,
    gluesql_core::{
        prelude::{Error, Glue, Result},
        store::{Store, StoreMut},
    },
    gluesql_memory_storage::MemoryStorage,
};

#[test]
fn error() {
    let storage = CompositeStorage::new();

    assert_eq!(
        block_on(storage.scan_data("Foo")).map(|_| ()),
        Err(Error::StorageMsg(
            "engine not found for table: Foo".to_owned()
        ))
    );

    let mut glue = Glue::new(storage);
    assert_eq!(
        glue.execute("CREATE TABLE Foo ENGINE = NONAME;"),
        Err(Error::StorageMsg(
            "storage not found for table: Foo".to_owned()
        ))
    );

    let storage = {
        let storage = MemoryStorage::default();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE WrongEngine (id INTEGER) ENGINE = SomethingElse")
            .unwrap();

        glue.storage
    };

    glue.storage.push("Test", storage);
    glue.storage.set_default("Test");

    glue.execute("CREATE TABLE Foo (id INTEGER);").unwrap();

    assert_eq!(
        block_on(glue.storage.scan_data("WrongEngine")).map(|_| ()),
        Err(Error::StorageMsg(
            "[fetch_storage] storage not found for table: WrongEngine".to_owned()
        ))
    );

    assert_eq!(
        block_on(glue.storage.delete_schema("WrongEngine")).map(|_| ()),
        Err(Error::StorageMsg(
            "[fetch_storage_mut] storage not found for table: WrongEngine".to_owned()
        ))
    );
}

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).unwrap();
    };
}

macro_rules! test {
    ($glue: ident $sql: literal, $result: expr) => {
        assert_eq!($glue.execute($sql), $result);
    };
}

#[test]
fn composite_storage_index() {
    use {gluesql_core::store::Index, gluesql_memory_storage::MemoryStorage};

    let mut storage = CompositeStorage::default();
    storage.push("mem", MemoryStorage::default());
    storage.set_default("mem");

    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Idx (id INTEGER);");

    assert_eq!(
        block_on(glue.storage.scan_data("Idx"))
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .as_ref()
            .map(Vec::len),
        Ok(0),
    );

    assert_eq!(
        block_on(glue.storage.scan_indexed_data("Idx", "hello", None, None)).map(|_| ()),
        Err(Error::StorageMsg(
            "[Storage] Index::scan_indexed_data is not supported".to_owned()
        ))
    );

    test!(
        glue "CREATE INDEX idx_id ON Idx (id);",
        Err(Error::StorageMsg("[Storage] Index::create_index is not supported".to_owned()))
    );
    test!(
        glue "DROP INDEX Idx.idx_id;",
        Err(Error::StorageMsg("[Storage] Index::drop_index is not supported".to_owned()))
    );
}

#[test]
fn composite_storage_function() {
    let storage = CompositeStorage::default();
    let mut glue = Glue::new(storage);

    test!(
        glue "CREATE FUNCTION abc() RETURN 1;",
        Err(Error::StorageMsg("[Storage] CustomFunction is not supported".to_owned()))
    );
    test!(
        glue "SELECT abc();",
        Err(Error::StorageMsg("[Storage] CustomFunction is not supported".to_owned()))
    );
    test!(
        glue "DROP FUNCTION abc;",
        Err(Error::StorageMsg("[Storage] CustomFunction is not supported".to_owned()))
    );
    test!(
        glue "SHOW FUNCTIONS;",
        Err(Error::StorageMsg("[Storage] CustomFunction is not supported".to_owned()))
    );
}
