use {
    async_trait::async_trait, futures::stream::TryStreamExt, gluesql_core::prelude::Glue,
    gluesql_memory_storage::MemoryStorage, test_suite::*,
};

struct MemoryTester {
    glue: Glue<MemoryStorage>,
}

#[async_trait(?Send)]
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

generate_alter_table_tests!(tokio::test, MemoryTester);

generate_metadata_table_tests!(tokio::test, MemoryTester);

generate_custom_function_tests!(tokio::test, MemoryTester);

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).await.unwrap();
    };
}

macro_rules! test {
    ($glue: ident $sql: expr, $result: expr) => {
        assert_eq!($glue.execute($sql).await, $result);
    };
}

#[tokio::test]
async fn memory_storage_index() {
    use gluesql_core::{
        prelude::{Error, Glue},
        store::{Index, Store},
    };

    let storage = MemoryStorage::default();

    assert_eq!(
        storage
            .scan_data("Idx")
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .as_ref()
            .map(Vec::len),
        Ok(0),
    );

    assert_eq!(
        storage
            .scan_indexed_data("Idx", "hello", None, None)
            .await
            .map(|_| ()),
        Err(Error::StorageMsg(
            "[MemoryStorage] index is not supported".to_owned()
        ))
    );

    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Idx (id INTEGER);");
    test!(
        glue "CREATE INDEX idx_id ON Idx (id);",
        Err(Error::StorageMsg("[MemoryStorage] index is not supported".to_owned()))
    );
    test!(
        glue "DROP INDEX Idx.idx_id;",
        Err(Error::StorageMsg("[MemoryStorage] index is not supported".to_owned()))
    );
}

#[tokio::test]
async fn memory_storage_transaction() {
    use gluesql_core::prelude::{Error, Glue, Payload};

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE TxTest (id INTEGER);");
    test!(glue "BEGIN", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "COMMIT", Ok(vec![Payload::Commit]));
    test!(glue "ROLLBACK", Ok(vec![Payload::Rollback]));
}
