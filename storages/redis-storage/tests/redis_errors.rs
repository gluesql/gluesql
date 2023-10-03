use gluesql_redis_storage::RedisStorage;

use {
    async_trait::async_trait,
    gluesql_core::prelude::{Error, Glue, Payload},
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

macro_rules! test {
    ($glue: ident $sql: expr, $result: expr) => {
        assert_eq!($glue.execute($sql).await, $result);
    };
}

#[tokio::test]
async fn redis_storage_errors() {
    use gluesql_core::prelude::Glue;
    let url = "localhost";
    let port: u16 = 6379;

    let storage = RedisStorage::new("redis_storage_namespace_first", url, port);
    let mut glue = Glue::new(storage);

    exec!(glue "DROP TABLE IF EXISTS dummy;");

    exec!(glue "CREATE TABLE dummy (id INTEGER PRIMARY KEY);");
    exec!(glue "INSERT INTO dummy (id) values (1)");
    exec!(glue "INSERT INTO dummy (id) values (11)");

    // index
    test!(
        glue "CREATE INDEX idx_id ON dummy (id);",
        Err(Error::StorageMsg("[RedisStorage] index is not supported".to_owned()))
    );
    test!(
        glue "DROP INDEX dummy.idx_id;",
        Err(Error::StorageMsg("[RedisStorage] index is not supported".to_owned()))
    );

    // transaction
    test!(
        glue "BEGIN",
        Err(Error::StorageMsg("[RedisStorage] transaction is not supported".to_owned()))
    );
    assert_eq!(
        glue.execute("COMMIT;").await.unwrap(),
        vec![Payload::Commit]
    );
    assert_eq!(
        glue.execute("ROLLBACK;").await.unwrap(),
        vec![Payload::Rollback]
    );

    exec!(glue "DROP TABLE dummy;");
}
