use gluesql_redis_storage::RedisStorage;

use {
    async_trait::async_trait,
    gluesql_core::prelude::{Glue, Payload, Value},
    redis::Commands,
    test_suite::*,
};

struct RedisStorageTester {
    glue: Glue<RedisStorage>,
}

/// MUST run redis locally before test
/// eg.) docker run --rm -p 6379:6379 redis
#[async_trait(?Send)]
impl Tester<RedisStorage> for RedisStorageTester {
    async fn new(namespace: &str) -> Self {
        let url = "localhost";
        let port: u16 = 6379;
        let storage = RedisStorage::new(namespace, url, port);

        // MUST clear namespace before test
        // DO NOT USE FLUSHALL command because it also flushes all namespaces of other clients.
        // It must clear only its own namespace.
        let key_iter: Vec<String> = storage
            .conn
            .borrow_mut()
            .scan_match(&format!("{}#*", namespace))
            .unwrap()
            .collect();
        for key in key_iter {
            let _: () = redis::cmd("DEL")
                .arg(&key)
                .query(&mut storage.conn.borrow_mut())
                .unwrap_or_else(|_| panic!("failed to execute DEL for key={}", key));
        }

        let glue = Glue::new(storage);
        RedisStorageTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<RedisStorage> {
        &mut self.glue
    }
}

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).await.unwrap();
    };
}

#[tokio::test]
async fn redis_storage_reconnect() {
    use gluesql_core::prelude::Glue;

    {
        let url = "localhost";
        let port: u16 = 6379;
        let storage = RedisStorage::new("redis_storage_reconnect", url, port);
        let mut glue = Glue::new(storage);

        exec!(glue "DROP TABLE IF EXISTS dummy;");

        exec!(glue "CREATE TABLE dummy (id INTEGER PRIMARY KEY);");
        exec!(glue "INSERT INTO dummy (id) values (1)");
        exec!(glue "INSERT INTO dummy (id) values (11)");
    }

    {
        let url = "localhost";
        let port: u16 = 6379;
        let storage = RedisStorage::new("redis_storage_reconnect", url, port);
        let mut glue = Glue::new(storage);

        let ret = glue.execute("SELECT * FROM dummy;").await.unwrap();
        match &ret[0] {
            Payload::Select { labels, rows } => {
                assert_eq!(labels[0], "id");
                assert_eq!(rows[0].len(), 1);
                assert_eq!(rows[1].len(), 1);
                assert_eq!(rows[0][0], Value::I64(1));
                assert_eq!(rows[1][0], Value::I64(11));
            }
            _ => unreachable!(),
        }
        exec!(glue "DROP TABLE dummy;");
    }
}

#[tokio::test]
async fn redis_storage_reconnect_drop() {
    use gluesql_core::prelude::Glue;

    {
        let url = "localhost";
        let port: u16 = 6379;
        let storage = RedisStorage::new("redis_storage_reconnect_drop", url, port);
        let mut glue = Glue::new(storage);

        exec!(glue "DROP TABLE IF EXISTS dummy;");

        exec!(glue "CREATE TABLE dummy (id INTEGER PRIMARY KEY);");
        exec!(glue "INSERT INTO dummy (id) values (1)");
        exec!(glue "INSERT INTO dummy (id) values (11)");
        exec!(glue "DROP TABLE dummy;");
    }

    {
        let url = "localhost";
        let port: u16 = 6379;
        let storage = RedisStorage::new("redis_storage_reconnect_drop", url, port);
        let mut glue = Glue::new(storage);

        if glue.execute("SELECT * FROM dummy;").await.is_ok() {
            exec!(glue "DROP TABLE IF EXISTS dummy;");
            panic!("SELECT should fail");
        }
    }
}
