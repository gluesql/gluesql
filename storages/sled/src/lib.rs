mod sled_storage;

use crate::sled_storage::SledStorage;

use gluesql::{execute, tests::Tester, Payload, Query, Result};

pub struct SledTester {
    storage: Option<SledStorage>,
}

impl Tester for SledTester {
    fn new(namespace: &str) -> Self {
        println!("{}", namespace);
        let path = format!("data/{}", namespace);

        match std::fs::remove_dir_all(&path) {
            Ok(()) => (),
            Err(e) => {
                println!("fs::remove_file {:?}", e);
            }
        }

        let storage = SledStorage::new(path).expect("SledStorage::new");
        let storage = Some(storage);

        SledTester { storage }
    }

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
