use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_parquet_storage::ParquetStorage,
    std::fs::remove_dir_all, test_suite::*,
};

struct ParquetTester {
    glue: Glue<ParquetStorage>,
}

#[async_trait(?Send)]
impl Tester<ParquetStorage> for ParquetTester {
    async fn new(namespace: &str) -> Self {
        let storage = ParquetStorage::new(Some(namespace.to_owned())).await.unwrap();
        let glue = Glue::new(storage);

        ParquetTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<ParquetStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, ParquetTester);
