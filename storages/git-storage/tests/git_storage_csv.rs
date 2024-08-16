use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_csv_storage::CsvStorage,
    gluesql_git_storage::GitStorage, std::fs::remove_dir_all, test_suite::*,
};

struct GitStorageTester {
    glue: Glue<GitStorage<CsvStorage>>,
}

#[async_trait(?Send)]
impl Tester<GitStorage<CsvStorage>> for GitStorageTester {
    async fn new(namespace: &str) -> Self {
        let path = format!("tmp/git_storage_csv/{namespace}");

        if let Err(e) = remove_dir_all(&path) {
            println!("fs::remove_file {:?}", e);
        };

        let storage = GitStorage::init(&path).expect("GitStorage::init - CSV");
        let glue = Glue::new(storage);
        GitStorageTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<GitStorage<CsvStorage>> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, GitStorageTester);
