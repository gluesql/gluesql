use {gluesql_core::prelude::Glue, gluesql_sqlite_storage::SqliteStorage, test_suite::*};

struct SqliteTester {
    glue: Glue<SqliteStorage>,
}

impl Tester<SqliteStorage> for SqliteTester {
    fn new(_namespace: &str) -> Self {
        Self {
            glue: Glue::new(SqliteStorage::in_memory().unwrap()),
        }
    }

    fn get_glue(&mut self) -> &mut Glue<SqliteStorage> {
        &mut self.glue
    }
}

generate_store_tests!(test, SqliteTester);
generate_transaction_tests!(test, SqliteTester);
