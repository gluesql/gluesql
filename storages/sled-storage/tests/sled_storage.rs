use {
    gluesql_sled_storage::SledStorage,
    std::{cell::RefCell, rc::Rc},
    test_suite::*,
};

struct SledTester {
    storage: Rc<RefCell<Option<SledStorage>>>,
}

impl Tester<SledStorage> for SledTester {
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

// generate_store_tests!(tokio::test, SledTester);
// generate_index_tests!(tokio::test, SledTester);
// generate_transaction_tests!(tokio::test, SledTester);
// generate_alter_table_tests!(tokio::test, SledTester);
// generate_alter_table_index_tests!(tokio::test, SledTester);
// generate_transaction_alter_table_tests!(tokio::test, SledTester);
// generate_transaction_index_tests!(tokio::test, SledTester);
// generate_metadata_tests!(tokio::test, SledTester);
// generate_transaction_metadata_tests!(tokio::test, SledTester);
