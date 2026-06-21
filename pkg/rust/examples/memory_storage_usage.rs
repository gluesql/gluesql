#[cfg(feature = "gluesql_memory_storage")]
mod api_usage {
    use gluesql::{gluesql_memory_storage::MemoryStorage, prelude::Glue};

    pub fn run() {
        let storage = MemoryStorage::default();
        let mut glue = Glue::new(storage);

        glue.execute("DROP TABLE IF EXISTS api_test").unwrap();

        glue.execute(
            "CREATE TABLE api_test (
                id INTEGER,
                name TEXT,
                nullable TEXT NULL,
                is BOOLEAN
            )",
        )
        .unwrap();

        glue.execute(
            "INSERT INTO api_test (
                id,
                name,
                nullable,
                is
            ) VALUES
                (1, 'test1', 'not null', TRUE),
                (2, 'test2', NULL, FALSE)",
        )
        .unwrap();
    }
}

fn main() {
    #[cfg(feature = "gluesql_memory_storage")]
    api_usage::run();
}
