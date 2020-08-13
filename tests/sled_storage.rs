#[cfg(feature = "sled-storage")]
use std::convert::TryFrom;

#[cfg(feature = "sled-storage")]
use gluesql::{execute, generate_tests, sled, tests::*, Payload, Query, Result, SledStorage};

#[cfg(feature = "sled-storage")]
struct SledTester {
    storage: Option<SledStorage>,
}

#[cfg(feature = "sled-storage")]
impl Tester for SledTester {
    fn new(namespace: &str) -> Self {
        let path = format!("data/{}", namespace);

        match std::fs::remove_dir_all(&path) {
            Ok(()) => (),
            Err(e) => {
                println!("fs::remove_file {:?}", e);
            }
        }

        let config = sled::Config::default()
            .path(path)
            .temporary(true)
            .mode(sled::Mode::HighThroughput);
        let storage = SledStorage::try_from(config).expect("SledStorage::new");
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

#[cfg(feature = "sled-storage")]
generate_tests!(test, SledTester);
