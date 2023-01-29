use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_memory_storage::MemoryStorage,
    test_suite::*,
};

struct MemoryTester {
    glue: Glue<MemoryStorage>,
}

#[async_trait]
impl Tester<MemoryStorage> for MemoryTester {
    async fn new(_: &str) -> Self {
        let storage = MemoryStorage::default();
        let glue = Glue::new(storage);

        MemoryTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<MemoryStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, MemoryTester);

#[cfg(feature = "alter-table")]
generate_alter_table_tests!(tokio::test, MemoryTester);

#[cfg(any(feature = "alter-table", feature = "index"))]
macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).unwrap();
    };
}

#[cfg(any(feature = "alter-table", feature = "index"))]
macro_rules! test {
    ($glue: ident $sql: literal, $result: expr) => {
        assert_eq!($glue.execute($sql), $result);
    };
}

#[cfg(feature = "index")]
#[test]
fn memory_storage_index() {
    use {
        futures::executor::block_on,
        gluesql_core::{
            prelude::Glue,
            result::{Error, Result},
            store::{Index, Store},
        },
    };

    let storage = MemoryStorage::default();

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
            "[MemoryStorage] Index::scan_indexed_data is not supported".to_owned()
        ))
    );

    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Idx (id INTEGER);");
    test!(
        glue "CREATE INDEX idx_id ON Idx (id);",
        Err(Error::StorageMsg("[MemoryStorage] Index::create_index is not supported".to_owned()))
    );
    test!(
        glue "DROP INDEX Idx.idx_id;",
        Err(Error::StorageMsg("[MemoryStorage] Index::drop_index is not supported".to_owned()))
    );
}

#[cfg(feature = "transaction")]
#[test]
fn memory_storage_transaction() {
    use gluesql_core::{prelude::Glue, result::Error};

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE TxTest (id INTEGER);");
    test!(glue "BEGIN", Err(Error::StorageMsg("[MemoryStorage] Transaction::begin is not supported".to_owned())));
    test!(glue "COMMIT", Err(Error::StorageMsg("[MemoryStorage] Transaction::commit is not supported".to_owned())));
    test!(glue "ROLLBACK", Err(Error::StorageMsg("[MemoryStorage] Transaction::rollback is not supported".to_owned())));
}
