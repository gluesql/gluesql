#![cfg(feature = "test-redis")]

use {
    async_trait::async_trait,
    gluesql_core::prelude::Glue,
    gluesql_redis_storage::RedisStorage,
    redis::Commands,
    std::{env, fs},
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
        let mut path = env::current_dir().unwrap();
        path.push("tests/redis-storage.toml");
        let redis_config_str = fs::read_to_string(path).unwrap();
        let redis_config: toml::Value = toml::from_str(&redis_config_str).unwrap();
        let url = redis_config["redis"]["url"].as_str().unwrap();
        let port: u16 = redis_config["redis"]["port"].as_integer().unwrap() as u16;

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

generate_store_tests!(tokio::test, RedisStorageTester);
generate_alter_table_tests!(tokio::test, RedisStorageTester);
generate_metadata_table_tests!(tokio::test, RedisStorageTester);

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).await.unwrap();
    };
}

/// check if it's able to create tables with same name in different namespace
#[tokio::test]
async fn redis_storage_namespace() {
    use gluesql_core::prelude::Glue;

    let mut path = env::current_dir().unwrap();
    path.push("tests/redis-storage.toml");
    let redis_config_str = fs::read_to_string(path).unwrap();
    let redis_config: toml::Value = toml::from_str(&redis_config_str).unwrap();
    let url = redis_config["redis"]["url"].as_str().unwrap();
    let port: u16 = redis_config["redis"]["port"].as_integer().unwrap() as u16;

    let storage_first = RedisStorage::new("redis_storage_namespace_first", url, port);
    let mut glue_first = Glue::new(storage_first);

    let storage_second = RedisStorage::new("redis_storage_namespace_second", url, port);
    let mut glue_second = Glue::new(storage_second);

    exec!(glue_first "CREATE TABLE dummy (id INTEGER PRIMARY KEY);");
    exec!(glue_first "INSERT INTO dummy (id) values (1)");
    exec!(glue_first "INSERT INTO dummy (id) values (11)");

    exec!(glue_second "CREATE TABLE dummy (id INTEGER PRIMARY KEY);");
    exec!(glue_second "INSERT INTO dummy (id) values (2)");
    exec!(glue_second "INSERT INTO dummy (id) values (22)");

    exec!(glue_first "DROP TABLE dummy;");
    exec!(glue_second "DROP TABLE dummy;");
}

#[tokio::test]
async fn redis_storage_no_primarykey() {
    use {
        chrono::NaiveDate,
        gluesql_core::prelude::{Glue, Payload, Value},
    };

    let mut path = env::current_dir().unwrap();
    path.push("tests/redis-storage.toml");
    let redis_config_str = fs::read_to_string(path).unwrap();
    let redis_config: toml::Value = toml::from_str(&redis_config_str).unwrap();
    let url = redis_config["redis"]["url"].as_str().unwrap();
    let port: u16 = redis_config["redis"]["port"].as_integer().unwrap() as u16;

    let storage = RedisStorage::new("redis_storage_no_primarykey", url, port);
    let mut glue = Glue::new(storage);

    exec!(glue "DROP TABLE IF EXISTS Heroes;");
    exec!(glue "CREATE TABLE Heroes (id INTEGER, name TEXT, birth DATE);");
    exec!(glue r#"INSERT INTO Heroes (id, name, birth) values (1, 'Superman', '2023-12-31');"#);

    let ret: Vec<Payload> = glue.execute("SELECT * FROM Heroes;").await.unwrap();
    match &ret[0] {
        Payload::Select { labels, rows } => {
            assert_eq!(labels[0], "id");
            assert_eq!(rows[0][0], Value::I64(1));
            assert_eq!(labels[1], "name");
            assert_eq!(rows[0][1], Value::Str("Superman".to_owned()));
            assert_eq!(labels[2], "birth");
            assert_eq!(
                rows[0][2],
                Value::Date(NaiveDate::from_ymd_opt(2023, 12, 31).unwrap())
            );
        }
        _ => unreachable!(),
    }

    exec!(glue r#"INSERT INTO Heroes (id, name, birth) values (2, 'Batman', '2000-12-31');"#);
    exec!(glue r#"INSERT INTO Heroes (id, name, birth) values (3, 'Flash', '2011-12-31');"#);
    exec!(glue r#"UPDATE Heroes set birth = '1999-12-31' WHERE name = 'Batman';"#);

    let ret: Vec<Payload> = glue
        .execute("SELECT id, name, birth FROM Heroes;")
        .await
        .unwrap();
    match &ret[0] {
        Payload::Select { labels, rows } => {
            assert_eq!(labels[0], "id");
            assert_eq!(labels[1], "name");
            assert_eq!(labels[2], "birth");

            for r in rows.iter() {
                if r[1] == Value::Str("Batman".to_owned()) {
                    assert_eq!(
                        r[2],
                        Value::Date(NaiveDate::from_ymd_opt(1999, 12, 31).unwrap())
                    );
                }
            }
        }
        _ => unreachable!(),
    }

    exec!(glue r#"DELETE FROM Heroes WHERE name = 'Superman';"#);

    let ret: Vec<Payload> = glue
        .execute("SELECT id, name, birth FROM Heroes;")
        .await
        .unwrap();

    match &ret[0] {
        Payload::Select { labels, rows } => {
            assert_eq!(labels[0], "id");
            assert_eq!(labels[1], "name");
            assert_eq!(labels[2], "birth");

            for r in rows.iter() {
                assert_ne!(r[1], Value::Str("Super".to_owned()));
            }
        }
        _ => unreachable!(),
    }

    let _: Vec<Payload> = glue
        .execute("SELECT id, name, birth FROM Heroes WHERE id = '2';")
        .await
        .unwrap();

    exec!(glue "DROP TABLE Heroes;");
}

#[tokio::test]
async fn redis_storage_primarykey() {
    use {
        chrono::NaiveDate,
        gluesql_core::prelude::{Glue, Payload, Value},
    };

    let mut path = env::current_dir().unwrap();
    path.push("tests/redis-storage.toml");
    let redis_config_str = fs::read_to_string(path).unwrap();
    let redis_config: toml::Value = toml::from_str(&redis_config_str).unwrap();
    let url = redis_config["redis"]["url"].as_str().unwrap();
    let port: u16 = redis_config["redis"]["port"].as_integer().unwrap() as u16;

    let storage = RedisStorage::new("redis_storage_primarykey", url, port);
    let mut glue = Glue::new(storage);

    exec!(glue "DROP TABLE IF EXISTS Heroes;");
    exec!(glue "CREATE TABLE Heroes (name TEXT PRIMARY KEY, birth DATE);");

    // INSERT with PRIMARY KEY will test the insert_data method
    // Any type can be the PRIMARY KEY
    exec!(glue r#"INSERT INTO Heroes (name, birth) values ('Superman', '2023-12-31');"#);
    exec!(glue r#"INSERT INTO Heroes (name, birth) values ('Batman', '2011-12-31');"#);

    // SELECT with PRIMARY KEY will test the fetch_data method
    let ret: Vec<Payload> = glue
        .execute("SELECT name, birth FROM Heroes;")
        .await
        .unwrap();
    match &ret[0] {
        Payload::Select { labels, rows } => {
            assert_eq!(labels[0], "name");
            assert_eq!(labels[1], "birth");

            assert_eq!(rows.len(), 2);
        }
        _ => unreachable!(),
    }

    exec!(glue r#"DELETE FROM Heroes WHERE name = 'Superman';"#);

    let ret: Vec<Payload> = glue
        .execute("SELECT name, birth FROM Heroes;")
        .await
        .unwrap();
    match &ret[0] {
        Payload::Select { labels, rows } => {
            assert_eq!(labels[0], "name");
            assert_eq!(labels[1], "birth");

            assert_eq!(rows.len(), 1);

            assert_eq!(rows[0][0], Value::Str("Batman".to_owned()));
            assert_eq!(
                rows[0][1],
                Value::Date(NaiveDate::from_ymd_opt(2011, 12, 31).unwrap())
            );
        }
        _ => unreachable!(),
    }

    exec!(glue "DROP TABLE Heroes;");
}
