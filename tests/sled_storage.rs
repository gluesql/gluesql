#![cfg(feature = "sled-storage")]

use {
    cfg_if::cfg_if,
    std::{cell::RefCell, convert::TryFrom, rc::Rc},
};

use gluesql::{sled::IVec, sled_storage::SledStorage, tests::*, *};

struct SledTester {
    storage: Rc<RefCell<Option<SledStorage>>>,
}

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

generate_store_tests!(tokio::test, SledTester);
generate_index_tests!(tokio::test, SledTester);
generate_transaction_tests!(tokio::test, SledTester);

cfg_if! {
    if #[cfg(feature = "alter-table")] {
        generate_alter_table_tests!(tokio::test, SledTester);
        generate_alter_table_index_tests!(tokio::test, SledTester);
        generate_transaction_alter_table_tests!(tokio::test, SledTester);
        generate_transaction_index_tests!(tokio::test, SledTester);
    }
}
