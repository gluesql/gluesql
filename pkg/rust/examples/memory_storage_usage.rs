#[cfg(feature = "memory-storage")]
mod api_usage {
    use gluesql::{memory_storage::MemoryStorage, prelude::Glue};

    fn memory_basic() {
        let storage = MemoryStorage::default();
        let mut glue = Glue::new(storage);

        glue.execute("DROP TABLE IF EXISTS api_test");

        glue.execute(
            "CREATE TABLE api_test (
                id INTEGER,
                name TEXT,
                nullable TEXT NULL,
                is BOOLEAN
            )",
        );

        glue.execute(
            "INSERT INTO api_test (
                id,
                name,
                nullable,
                is
            ) VALUES
                (1, 'test1', 'not null', TRUE),
                (2, 'test2', NULL, FALSE)",
        );
    }

    fn memory_basic_async() {
        use futures::executor::block_on;

        let storage = MemoryStorage::default();
        let mut glue = Glue::new(storage);

        block_on(async {
            glue.execute_async("DROP TABLE IF EXISTS api_test").await;

            glue.execute_async(
                "CREATE TABLE api_test (
                    id INTEGER,
                    name TEXT,
                    nullable TEXT NULL,
                    is BOOLEAN
                )",
            )
            .await;
        });
    }

    pub fn run() {
        memory_basic();
        memory_basic_async();
    }
}

fn main() {
    #[cfg(feature = "memory-storage")]
    api_usage::run();
}
