// use {
//     async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_file_storage::FileStorage,
//     gluesql_git_storage::GitStorage, std::fs::remove_dir_all, test_suite::*,
// };

// struct GitStorageTester {
//     glue: Glue<GitStorage<FileStorage>>,
// }

// #[async_trait(?Send)]
// impl Tester<GitStorage<FileStorage>> for GitStorageTester {
//     async fn new(namespace: &str) -> Self {
//         let path = format!("tmp/git_storage_file/{namespace}");

//         if let Err(e) = remove_dir_all(&path) {
//             println!("fs::remove_file {:?}", e);
//         };

//         let storage = GitStorage::init(&path).expect("GitStorage::init - File");
//         let glue = Glue::new(storage);
//         GitStorageTester { glue }
//     }

//     fn get_glue(&mut self) -> &mut Glue<GitStorage<FileStorage>> {
//         &mut self.glue
//     }
// }

// generate_store_tests!(tokio::test, GitStorageTester);
