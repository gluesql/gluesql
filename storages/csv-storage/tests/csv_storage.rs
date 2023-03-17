use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_csv_storage::CsvStorage,
    std::fs::remove_dir_all, test_suite::*,
};

struct CsvStorageTester {
    glue: Glue<CsvStorage>,
}

#[async_trait(?Send)]
impl Tester<CsvStorage> for CsvStorageTester {
    async fn new(namespace: &str) -> Self {
        let csv_path = format!("tmp/{namespace}");

        if let Err(e) = remove_dir_all(&csv_path) {
            println!("fs::remove_file {:?}", e);
        };

        let storage =
            CsvStorage::from_toml("test_schema.toml").expect("Incorrect schema file path");
        let glue = Glue::new(storage);

        CsvStorageTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<CsvStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, CsvStorageTester);
