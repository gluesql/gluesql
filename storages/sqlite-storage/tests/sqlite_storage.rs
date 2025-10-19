use {
    async_trait::async_trait,
    gluesql_core::prelude::Glue,
    gluesql_sqlite_storage::SqliteStorage,
    std::{fs, path::PathBuf},
    test_suite::*,
};

struct SqliteTester {
    glue: Glue<SqliteStorage>,
    db_path: PathBuf,
}

#[async_trait(?Send)]
impl Tester<SqliteStorage> for SqliteTester {
    async fn new(namespace: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push("gluesql-sqlite-tests");
        fs::create_dir_all(&path).expect("create sqlite test directory");

        path.push(format!("{namespace}.db"));
        let _ = fs::remove_file(&path);

        let storage = SqliteStorage::new(&path).await.expect("SqliteStorage::new");
        let glue = Glue::new(storage);

        Self {
            glue,
            db_path: path,
        }
    }

    fn get_glue(&mut self) -> &mut Glue<SqliteStorage> {
        &mut self.glue
    }
}

impl Drop for SqliteTester {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.db_path);
    }
}

generate_store_tests!(tokio::test, SqliteTester);
generate_transaction_tests!(tokio::test, SqliteTester);
