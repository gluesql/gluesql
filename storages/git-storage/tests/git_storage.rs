use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_git_storage::GitStorage,
    std::fs::remove_dir_all, test_suite::*,
};

struct GitStorageTester {
    glue: Glue<GitStorage>,
}

#[async_trait(?Send)]
impl Tester<GitStorage> for GitStorageTester {
    async fn new(namespace: &str) -> Self {
        let path = format!("tmp/{namespace}");

        if let Err(e) = remove_dir_all(&path) {
            println!("fs::remove_file {:?}", e);
        };

        let storage = GitStorage::new(&path).expect("GitStorage::new");
        let glue = Glue::new(storage);
        GitStorageTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<GitStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, GitStorageTester);
