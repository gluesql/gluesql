mod memory_storage;

use gluesql::store::Store;
use gluesql::Tester;

use memory_storage::{DataKey, MemoryStorage};

pub struct MemoryTester {
    storage: MemoryStorage,
}

impl MemoryTester {
    pub fn new() -> Self {
        let storage = MemoryStorage::new().expect("MemoryStorage::new");

        MemoryTester { storage }
    }
}

impl Default for MemoryTester {
    fn default() -> Self {
        Self::new()
    }
}

impl Tester<DataKey> for MemoryTester {
    fn get_storage(&mut self) -> &mut dyn Store<DataKey> {
        &mut self.storage
    }
}
