use {
    async_trait::async_trait,
    gluesql_core::{data::Value::I64, prelude::Glue},
    gluesql_file_storage::FileStorage,
    std::fs::{create_dir, remove_dir_all},
    test_suite::*,
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
generate_alter_table_tests!(tokio::test, FileStorageTester);

#[tokio::test]
async fn scan_data_to_ignore_directory_items() {
    let path = "./tests/ignore_directory_items/";
    let storage = FileStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Foo (id INTEGER);")
        .await
        .unwrap();
    glue.execute("INSERT TABLE Foo VALUES (1), (2), (3);")
        .await
        .unwrap();

    let dir_path = format!("{path}Foo/something_in_data_directory");
    create_dir(dir_path).unwrap();

    assert_eq!(
        glue.execute("SELECT * FROM Foo").await.unwrap().remove(0),
        select!(id I64; 1; 2; 3)
    );

    remove_dir_all(path).unwrap();
}
