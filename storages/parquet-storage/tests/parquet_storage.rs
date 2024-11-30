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
        let path: String = format!("tmp/{namespace}");

        if let Err(e) = remove_dir_all(&path) {
            println!("fs::remove_file {:?}", e);
        }
        let storage = ParquetStorage::new(&path).expect("ParquetStorage::new");
        let glue = Glue::new(storage);

        ParquetTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<ParquetStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, ParquetTester);
generate_alter_table_tests!(tokio::test, ParquetTester);
