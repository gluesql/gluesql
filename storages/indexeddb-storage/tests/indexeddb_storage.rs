use {
    glueseql_indexeddb_storage::IndexeddbStorage,
    std::{cell::RefCell, rc::Rc},
    test_suite::*,
};

struct MemoryTester {
    storage: Rc<RefCell<Option<IndexeddbStorage>>>,
}

impl Tester<IndexeddbStorage> for MemoryTester {
    fn new(_: &str) -> Self {
        let storage = Some(IndexeddbStorage::default());
        let storage = Rc::new(RefCell::new(storage));

        MemoryTester { storage }
    }

    fn get_cell(&mut self) -> Rc<RefCell<Option<IndexeddbStorage>>> {
        Rc::clone(&self.storage)
    }
}

generate_store_tests!(tokio::test, MemoryTester);
