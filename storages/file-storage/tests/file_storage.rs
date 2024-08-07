use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_file_storage::FileStorage,
    std::fs::remove_dir_all, test_suite::*,
};

struct FileStorageTester {
    glue: Glue<FileStorage>,
}

#[async_trait(?Send)]
impl Tester<FileStorage> for FileStorageTester {
    async fn new(namespace: &str) -> Self {
        let path = format!("tmp/{namespace}");

        if let Err(e) = remove_dir_all(&path) {
            println!("fs::remove_file {:?}", e);
        };

        let storage = FileStorage::new(&path).expect("FileStorage::new");
        let glue = Glue::new(storage);
        FileStorageTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<FileStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, FileStorageTester);
