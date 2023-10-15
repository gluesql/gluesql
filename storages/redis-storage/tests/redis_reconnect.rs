use gluesql_redis_storage::RedisStorage;

use gluesql_core::prelude::{Payload, Value};

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).await.unwrap();
    };
}

/// MUST run redis locally before test
/// eg.) docker run --rm -p 6379:6379 redis
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
