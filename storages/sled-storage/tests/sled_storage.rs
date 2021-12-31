use {
    gluesql_test_suite::*,
    sled_storage::{
        sled::{self, IVec},
        SledStorage,
    },
    std::{cell::RefCell, rc::Rc},
};

struct SledTester {
    storage: Rc<RefCell<Option<SledStorage>>>,
}

impl Tester<IVec, SledStorage> for SledTester {
    fn new(namespace: &str) -> Self {
        let path = format!("data/{}", namespace);

        if let Err(e) = std::fs::remove_dir_all(&path) {
            eprintln!("fs::remove_file {:?}", e);
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

#[cfg(feature = "index")]
generate_index_tests!(tokio::test, SledTester);

#[cfg(feature = "transaction")]
generate_transaction_tests!(tokio::test, SledTester);

#[cfg(feature = "alter-table")]
generate_alter_table_tests!(tokio::test, SledTester);

#[cfg(all(feature = "alter-table", feature = "index"))]
generate_alter_table_index_tests!(tokio::test, SledTester);

#[cfg(all(feature = "transaction", feature = "alter-table"))]
generate_transaction_alter_table_tests!(tokio::test, SledTester);

#[cfg(all(feature = "transaction", feature = "index"))]
generate_transaction_index_tests!(tokio::test, SledTester);
