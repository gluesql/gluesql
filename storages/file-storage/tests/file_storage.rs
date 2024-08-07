use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_file_storage::FileStorage,
    test_suite::*,
};

struct FileStorageTester {
    glue: Glue<FileStorage>,
}

#[async_trait(?Send)]
impl Tester<FileStorage> for FileStorageTester {
    async fn new(_: &str) -> Self {
        let storage = FileStorage::default();
        let glue = Glue::new(storage);

        FileStorageTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<FileStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, FileStorageTester);
