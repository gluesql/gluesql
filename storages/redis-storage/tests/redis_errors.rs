#![cfg(feature = "test-redis")]

use {
    gluesql_core::prelude::{Error, Payload},
    gluesql_redis_storage::RedisStorage,
    std::{env, fs},
};

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

/// MUST run redis locally before test
/// eg.) docker run --rm -p 6379:6379 redis
#[tokio::test]
async fn redis_storage_errors() {
    use gluesql_core::prelude::Glue;

    let mut path = env::current_dir().unwrap();
    path.push("tests/redis-storage.toml");
    let redis_config_str = fs::read_to_string(path).unwrap();
    let redis_config: toml::Value = toml::from_str(&redis_config_str).unwrap();
    let url = redis_config["redis"]["url"].as_str().unwrap();
    let port: u16 = redis_config["redis"]["port"].as_integer().unwrap() as u16;

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
