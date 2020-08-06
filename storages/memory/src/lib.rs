mod memory_storage;
pub use memory_storage::MemoryStorage;

use gluesql::{execute, Payload, Query, Result, Tester};

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
    fn execute(&mut self, query: &Query) -> Result<Payload> {
        let storage = self.storage.take().unwrap();

        match execute(storage, query) {
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
