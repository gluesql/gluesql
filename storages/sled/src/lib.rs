mod sled_storage;
use sled_storage::SledStorage;

use gluesql::{execute, Payload, Result, TestQuery, Tester};

pub struct SledTester {
    storage: Option<SledStorage>,
}

impl SledTester {
    pub fn new(path: &str) -> Self {
        match std::fs::remove_dir_all(path) {
            Ok(()) => (),
            Err(e) => {
                println!("fs::remove_file {:?}", e);
            }
        }

        let storage = SledStorage::new(path.to_owned()).expect("SledStorage::new");
        let storage = Some(storage);

        SledTester { storage }
    }
}

impl Tester for SledTester {
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
