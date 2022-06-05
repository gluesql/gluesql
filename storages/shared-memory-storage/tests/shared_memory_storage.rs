use {
    gluesql_shared_memory_storage::SharedMemoryStorage,
    std::{cell::RefCell, rc::Rc},
    test_suite::*,
};

struct SharedMemoryTester {
    storage: Rc<RefCell<Option<SharedMemoryStorage>>>,
}

impl Tester<SharedMemoryStorage> for SharedMemoryTester {
    fn new(_: &str) -> Self {
        let storage = Some(SharedMemoryStorage::new());
        let storage = Rc::new(RefCell::new(storage));

        SharedMemoryTester { storage }
    }

    fn get_cell(&mut self) -> Rc<RefCell<Option<SharedMemoryStorage>>> {
        Rc::clone(&self.storage)
    }
}

generate_store_tests!(tokio::test, SharedMemoryTester);

generate_metadata_tests!(tokio::test, SharedMemoryTester);

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
    use futures::executor::block_on;
    use gluesql_core::{
        prelude::Glue,
        result::{Error, Result},
        store::{Index, Store},
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
            "[Shared MemoryStorage] index is not supported".to_owned()
        ))
    );

    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Idx (id INTEGER);");
    test!(
        glue "CREATE INDEX idx_id ON Idx (id);",
        Err(Error::StorageMsg("[Shared MemoryStorage] index is not supported".to_owned()))
    );
    test!(
        glue "DROP INDEX Idx.idx_id;",
        Err(Error::StorageMsg("[Shared MemoryStorage] index is not supported".to_owned()))
    );
}

#[test]
fn shared_memory_storage_transaction() {
    use gluesql_core::{prelude::Glue, result::Error};

    let storage = SharedMemoryStorage::new();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE TxTest (id INTEGER);");
    test!(glue "BEGIN", Err(Error::StorageMsg("[Shared MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "COMMIT", Err(Error::StorageMsg("[Shared MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "ROLLBACK", Err(Error::StorageMsg("[Shared MemoryStorage] transaction is not supported".to_owned())));
}

#[tokio::test]
async fn shared_memory_storage_thread() {
    use gluesql_core::prelude::{Glue, Payload};

    let storage = SharedMemoryStorage::new();

    let mut glue = Glue::new(storage.clone());
    exec!(glue "CREATE TABLE Thread (id INTEGER);");

    let thread_1 = tokio::spawn({
        // Arc::clone
        let storage = storage.clone();
        async {
            let mut glue = Glue::new(storage);
            exec!(glue "INSERT INTO Thread VALUES(1)");
        }
    });

    let thread_2 = tokio::spawn({
        // Arc::clone
        let storage = storage.clone();
        async {
            let mut glue = Glue::new(storage);
            exec!(glue "INSERT INTO Thread VALUES(2)");
        }
    });

    let _ = tokio::join!(thread_1, thread_2);

    let payloads = glue.execute("SELECT * FROM Thread").unwrap();
    assert_eq!(payloads.len(), 1);

    let payload = &payloads[0];
    let rows = match &payload {
        Payload::Select { rows, .. } => rows.iter().flatten().collect::<Vec<_>>(),
        _ => unreachable!(),
    };
    assert_eq!(rows.len(), 2);
}
