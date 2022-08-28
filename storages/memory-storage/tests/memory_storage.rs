use {
    gluesql_memory_storage::MemoryStorage,
    std::{cell::RefCell, rc::Rc},
    test_suite::*,
};

struct MemoryTester {
    storage: Rc<RefCell<Option<MemoryStorage>>>,
}

impl Tester<MemoryStorage> for MemoryTester {
    fn new(_: &str) -> Self {
        let storage = Some(MemoryStorage::default());
        let storage = Rc::new(RefCell::new(storage));

        MemoryTester { storage }
    }

    fn get_cell(&mut self) -> Rc<RefCell<Option<MemoryStorage>>> {
        Rc::clone(&self.storage)
    }
}

// generate_store_tests!(tokio::test, MemoryTester);

// generate_metadata_tests!(tokio::test, MemoryTester);

// generate_alter_table_tests!(tokio::test, MemoryTester);

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
fn memory_storage_index() {
    use futures::executor::block_on;
    use gluesql_core::{
        prelude::Glue,
        result::{Error, Result},
        store::{Index, Store},
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

#[test]
fn memory_storage_transaction() {
    use gluesql_core::{prelude::Glue, result::Error};

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE TxTest (id INTEGER);");
    test!(glue "BEGIN", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "COMMIT", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "ROLLBACK", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
}
