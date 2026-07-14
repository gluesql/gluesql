use {
    gluesql_core::prelude::Glue,
    gluesql_git_storage::{GitStorage, StorageType},
    std::fs::remove_dir_all,
    test_suite::*,
};

struct GitStorageTester {
    glue: Glue<GitStorage>,
}

impl Tester<GitStorage> for GitStorageTester {
    fn new(namespace: &str) -> Self {
        let path = format!("tmp/git_storage_csv/{namespace}");

        if let Err(e) = remove_dir_all(&path) {
            println!("fs::remove_file {e:?}");
        }

        let storage = GitStorage::init(&path, StorageType::Csv).expect("GitStorage::init - CSV");
        let glue = Glue::new(storage);
        GitStorageTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<GitStorage> {
        &mut self.glue
    }
}

generate_store_tests!(test, GitStorageTester);
generate_alter_table_tests!(test, GitStorageTester);
