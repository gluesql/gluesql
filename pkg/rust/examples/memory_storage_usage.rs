#[cfg(feature = "memory-storage")]
mod api_usage {
    use gluesql::{
        memory_storage::MemoryStorage,
        prelude::{Glue, Payload, Value},
    };

    fn memory_basic() {
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

        let queries = "SELECT * FROM api_test";
        let result = glue
            .execute(queries)
            .expect(&format!("Failed to execute query: {}", queries));
        assert_eq!(result.len(), 1);
        let rows = match &result[0] {
            Payload::Select { labels: _, rows } => rows,
            _ => panic!("Unexpected result: {:?}", result),
        };

        let mut row0 = rows[0].iter();
        assert_eq!(row0.next(), Some(&Value::I64(1)));
        assert_eq!(row0.next(), Some(&Value::Str("test1".to_string())));
        assert_eq!(row0.next(), Some(&Value::Str("not null".to_string())));
        assert_eq!(row0.next(), Some(&Value::Bool(true)));
        assert_eq!(row0.next(), None);
        let mut row1 = rows[1].iter();
        assert_eq!(row1.next(), Some(&Value::I64(2)));
        assert_eq!(row1.next(), Some(&Value::Str("test2".to_string())));
        // PartialEq for Null is not implemented.
        row1.next();
        assert_eq!(row1.next(), Some(&Value::Bool(false)));
        assert_eq!(row1.next(), None);
    }

    fn memory_basic_async() {
        use futures::executor::block_on;

        let storage = MemoryStorage::default();
        let mut glue = Glue::new(storage);

        block_on(async {
            glue.execute_async("DROP TABLE IF EXISTS api_test")
                .await
                .unwrap();

            glue.execute_async(
                "CREATE TABLE api_test (
                    id INTEGER,
                    name TEXT,
                    nullable TEXT NULL,
                    is BOOLEAN
                )",
            )
            .await
            .unwrap();
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
