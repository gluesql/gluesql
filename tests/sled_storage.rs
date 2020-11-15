#[cfg(feature = "sled-storage")]
use sled::IVec;
#[cfg(feature = "sled-storage")]
use std::{cell::RefCell, convert::TryFrom, rc::Rc};

#[cfg(feature = "sled-storage")]
use gluesql::{generate_alter_table_tests, generate_tests, sled, tests::*, SledStorage};

#[cfg(feature = "sled-storage")]
struct SledTester {
    storage: Rc<RefCell<Option<SledStorage>>>,
}

#[cfg(feature = "sled-storage")]
impl Tester<IVec, SledStorage> for SledTester {
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

        let storage = SledStorage::try_from(config)
            .map(Some)
            .map(RefCell::new)
            .map(Rc::new)
            .expect("SledStorage::new");

        SledTester { storage }
    }

    fn get_cell(&mut self) -> Rc<RefCell<Option<SledStorage>>> {
        Rc::clone(&self.storage)
    }
}

#[cfg(feature = "sled-storage")]
generate_tests!(tokio::test, SledTester);
