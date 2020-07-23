mod memory_storage;
use memory_storage::MemoryStorage;

use gluesql::{execute, Payload, Result, TestQuery, Tester};

pub struct MemoryTester {
    storage: Option<MemoryStorage>,
}

impl MemoryTester {
    pub fn new() -> Self {
        let storage = MemoryStorage::new().expect("MemoryStorage::new");
        let storage = Some(storage);

        Self { storage }
    }
}

impl Default for MemoryTester {
    fn default() -> Self {
        Self::new()
    }
}

impl Tester for MemoryTester {
    fn execute(&mut self, query: TestQuery) -> Result<Payload> {
        let storage = self.storage.take().unwrap();

        match execute(storage, query.0) {
            Ok((storage, payload)) => {
                self.storage = Some(storage);

                Ok(payload)
            }
            Err((storage, error)) => {
                self.storage = Some(storage);

                Err(error)
            }
        }
    }
}
