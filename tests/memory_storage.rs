#![cfg(feature = "memory-storage")]

use {
    futures::executor::block_on,
    std::{cell::RefCell, rc::Rc},
};

use gluesql::{
    declare_test_fn, generate_store_tests,
    memory_storage::Key,
    store::{Index, Store},
    tests::*,
    Error, Glue, MemoryStorage, Result,
};

struct MemoryTester {
    storage: Rc<RefCell<Option<MemoryStorage>>>,
}

impl Tester<Key, MemoryStorage> for MemoryTester {
    fn new(_: &str) -> Self {
        let storage = Some(MemoryStorage::default());
        let storage = Rc::new(RefCell::new(storage));

        MemoryTester { storage }
    }

    fn get_cell(&mut self) -> Rc<RefCell<Option<MemoryStorage>>> {
        Rc::clone(&self.storage)
    }
}

generate_store_tests!(tokio::test, MemoryTester);

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

#[cfg(feature = "alter-table")]
#[test]
fn memory_storage_alter_table() {
    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Footer (id INTEGER);");
    test!(
        glue "ALTER TABLE Footer RENAME TO Header;",
        Err(Error::StorageMsg("[MemoryStorage] alter-table is not supported".to_owned()))
    );
    test!(
        glue "ALTER TABLE Footer RENAME COLUMN id TO od;",
        Err(Error::StorageMsg("[MemoryStorage] alter-table is not supported".to_owned()))
    );
    test!(
        glue "ALTER TABLE Footer ADD COLUMN ratio FLOAT DEFAULT 1.0;",
        Err(Error::StorageMsg("[MemoryStorage] alter-table is not supported".to_owned()))
    );
    test!(
        glue "ALTER TABLE Footer DROP COLUMN id;",
        Err(Error::StorageMsg("[MemoryStorage] alter-table is not supported".to_owned()))
    );
}

#[cfg(feature = "index")]
#[test]
fn memory_storage_index() {
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

#[cfg(feature = "transaction")]
#[test]
fn memory_storage_transaction() {
    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    test!(glue "BEGIN", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "COMMIT", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "ROLLBACK", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
}
