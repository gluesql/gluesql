use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_git_storage::GitStorage,
    gluesql_json_storage::JsonStorage, std::fs::remove_dir_all, test_suite::*,
};

struct GitStorageTester {
    glue: Glue<GitStorage<JsonStorage>>,
}

#[async_trait(?Send)]
impl Tester<GitStorage<JsonStorage>> for GitStorageTester {
    async fn new(namespace: &str) -> Self {
        let path = format!("tmp/git_storage_json/{namespace}");

        if let Err(e) = remove_dir_all(&path) {
            println!("fs::remove_file {:?}", e);
        };

        let storage = GitStorage::init(&path).expect("GitStorage::init - JSON");
        let glue = Glue::new(storage);
        GitStorageTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<GitStorage<JsonStorage>> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, GitStorageTester);
