use {
    async_trait::async_trait, gluesql_core::prelude::Glue,
    gluesql_shared_memory_storage::SharedMemoryStorage, test_suite::*,
};

struct SharedMemoryTester {
    glue: Glue<SharedMemoryStorage>,
}

#[async_trait]
impl Tester<SharedMemoryStorage> for SharedMemoryTester {
    async fn new(_: &str) -> Self {
        let storage = SharedMemoryStorage::new();
        let glue = Glue::new(storage);

        SharedMemoryTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<SharedMemoryStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, SharedMemoryTester);

generate_alter_table_tests!(tokio::test, SharedMemoryTester);

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
fn shared_memory_storage_index() {
    use {
        futures::executor::block_on,
        gluesql_core::{
            prelude::Glue,
            result::{Error, Result},
            store::{Index, Store},
        },
    };

    let storage = SharedMemoryStorage::new();

    assert_eq!(
        block_on(storage.scan_data("Idx"))
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .as_ref()
            .map(Vec::len),
        Ok(0),
    );

    assert_eq!(
        block_on(storage.scan_indexed_data("Idx", "hello", None, None)).map(|_| ()),
        Err(Error::StorageMsg(
            "[SharedMemoryStorage] Index::scan_indexed_data is not supported".to_owned()
        ))
    );

    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Idx (id INTEGER);");
    test!(
        glue "CREATE INDEX idx_id ON Idx (id);",
        Err(Error::StorageMsg("[SharedMemoryStorage] Index::create_index is not supported".to_owned()))
    );
    test!(
        glue "DROP INDEX Idx.idx_id;",
        Err(Error::StorageMsg("[SharedMemoryStorage] Index::drop_index is not supported".to_owned()))
    );
}

#[test]
fn shared_memory_storage_transaction() {
    use gluesql_core::{prelude::Glue, result::Error};

    let storage = SharedMemoryStorage::new();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE TxTest (id INTEGER);");
    test!(glue "BEGIN", Err(Error::StorageMsg("[SharedMemoryStorage] Transaction::begin is not supported".to_owned())));
    test!(glue "COMMIT", Err(Error::StorageMsg("[SharedMemoryStorage] Transaction::commit is not supported".to_owned())));
    test!(glue "ROLLBACK", Err(Error::StorageMsg("[SharedMemoryStorage] Transaction::rollback is not supported".to_owned())));
}
