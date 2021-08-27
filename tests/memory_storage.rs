#![cfg(feature = "memory-storage")]

use std::{cell::RefCell, rc::Rc};

use gluesql::{generate_tests, memory_storage::Key, tests::*, MemoryStorage};

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

generate_tests!(tokio::test, MemoryTester);
