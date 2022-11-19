use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_sled_storage::SledStorage,
    test_suite::*,
};

struct SledTester {
    glue: Glue<SledStorage>,
}

#[async_trait(?Send)]
impl Tester<SledStorage> for SledTester {
    async fn new(namespace: &str) -> Self {
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
        let glue = Glue::new(storage);

        SledTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<SledStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, SledTester);
generate_index_tests!(tokio::test, SledTester);
generate_transaction_tests!(tokio::test, SledTester);
generate_alter_table_tests!(tokio::test, SledTester);
generate_alter_table_index_tests!(tokio::test, SledTester);
generate_transaction_alter_table_tests!(tokio::test, SledTester);
generate_transaction_index_tests!(tokio::test, SledTester);
generate_transaction_dictionary_tests!(tokio::test, SledTester);
