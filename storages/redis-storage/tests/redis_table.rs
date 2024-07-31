#![cfg(feature = "test-redis")]

use {
    gluesql_core::prelude::{Glue, Payload, Value},
    gluesql_redis_storage::RedisStorage,
    std::{env, fs},
};

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).await.unwrap();
    };
}

/// MUST run redis locally before test
/// eg.) docker run --rm -p 6379:6379 redis
#[tokio::test]
async fn redis_storage_tables() {
    use chrono::NaiveDate;

    let mut path = env::current_dir().unwrap();
    path.push("tests/redis-storage.toml");
    let redis_config_str = fs::read_to_string(path).unwrap();
    let redis_config: toml::Value = toml::from_str(&redis_config_str).unwrap();
    let url = redis_config["redis"]["url"].as_str().unwrap();
    let port: u16 = redis_config["redis"]["port"].as_integer().unwrap() as u16;

    let storage = RedisStorage::new("redis_storage_tables", url, port);
    let mut glue = Glue::new(storage);

    exec!(glue "DROP TABLE IF EXISTS DC;");
    exec!(glue "DROP TABLE IF EXISTS Marvels;");

    exec!(glue "CREATE TABLE DC (id INTEGER, name TEXT, birth DATE);");
    exec!(glue r#"INSERT INTO DC (id, name, birth) values (1, 'Superman', '2023-12-31');"#);
    exec!(glue r#"INSERT INTO DC (id, name, birth) values (2, 'Flash', '2011-12-31');"#);

    exec!(glue "CREATE TABLE Marvels (id INTEGER, name TEXT);");
    exec!(glue r#"INSERT INTO Marvels (id, name) values (1, 'Ironman');"#);
    exec!(glue r#"INSERT INTO Marvels (id, name) values (2, 'Wanda');"#);

    let ret: Vec<Payload> = glue
        .execute("SELECT id, name FROM Marvels WHERE name = 'Wanda';")
        .await
        .unwrap();
    match &ret[0] {
        Payload::Select { labels, rows } => {
            assert_eq!(labels[0], "id");
            assert_eq!(labels[1], "name");

            assert_eq!(rows[0][0], Value::I64(2));
            assert_eq!(rows[0][1], Value::Str("Wanda".to_owned()));
        }
        _ => unreachable!(),
    }

    exec!(glue "DROP TABLE Marvels;");
    assert!(glue.execute("SELECT id, name FROM Marvels;").await.is_err());

    let ret: Vec<Payload> = glue
        .execute("SELECT id, name, birth FROM DC WHERE name = 'Superman';")
        .await
        .unwrap();
    match &ret[0] {
        Payload::Select { labels, rows } => {
            assert_eq!(labels[0], "id");
            assert_eq!(labels[1], "name");
            assert_eq!(labels[2], "birth");

            assert_eq!(rows[0][0], Value::I64(1));
            assert_eq!(rows[0][1], Value::Str("Superman".to_owned()));
            assert_eq!(
                rows[0][2],
                Value::Date(NaiveDate::from_ymd_opt(2023, 12, 31).unwrap())
            );
        }
        _ => unreachable!(),
    }

    exec!(glue "DROP TABLE DC;");
}

#[tokio::test]
async fn redis_storage_add_column() {
    let mut path = env::current_dir().unwrap();
    path.push("tests/redis-storage.toml");
    let redis_config_str = fs::read_to_string(path).unwrap();
    let redis_config: toml::Value = toml::from_str(&redis_config_str).unwrap();
    let url = redis_config["redis"]["url"].as_str().unwrap();
    let port: u16 = redis_config["redis"]["port"].as_integer().unwrap() as u16;

    let storage = RedisStorage::new("redis_storage_add_column", url, port);
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE dummy (id INTEGER PRIMARY KEY);");
    exec!(glue "INSERT INTO dummy (id) values (1)");
    exec!(glue "INSERT INTO dummy (id) values (11)");
    exec!(glue "ALTER TABLE dummy ADD newcol TEXT");

    let ret: Vec<Payload> = glue.execute("SELECT id, newcol FROM dummy;").await.unwrap();
    match &ret[0] {
        Payload::Select { labels, rows } => {
            assert_eq!(labels[0], "id");
            assert_eq!(labels[1], "newcol");
            assert_eq!(rows[0][0], Value::I64(1));
            assert_eq!(rows[0][1], Value::Null);
            assert_eq!(rows[1][0], Value::I64(11));
            assert_eq!(rows[0][1], Value::Null);
        }
        _ => unreachable!(),
    }

    exec!(glue "DROP TABLE dummy;");

    // TABLE dummy was dropped.
    // It should be able to create the same namespace again.
    let storage = RedisStorage::new("redis_Storage_add_column", url, port);
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE dummy (id INTEGER);");
    exec!(glue "INSERT INTO dummy (id) values (1)");
    exec!(glue "INSERT INTO dummy (id) values (11)");
    exec!(glue "ALTER TABLE dummy ADD newcol TEXT");

    let ret: Vec<Payload> = glue.execute("SELECT id, newcol FROM dummy;").await.unwrap();
    match &ret[0] {
        Payload::Select { labels, rows } => {
            assert_eq!(labels[0], "id");
            assert_eq!(labels[1], "newcol");
            assert_eq!(rows[0][0], Value::I64(1));
            assert_eq!(rows[0][1], Value::Null);
            assert_eq!(rows[1][0], Value::I64(11));
            assert_eq!(rows[0][1], Value::Null);
        }
        _ => unreachable!(),
    }

    exec!(glue "DROP TABLE dummy;");
}

#[tokio::test]
async fn redis_storage_drop_column() {
    let mut path = env::current_dir().unwrap();
    path.push("tests/redis-storage.toml");
    let redis_config_str = fs::read_to_string(path).unwrap();
    let redis_config: toml::Value = toml::from_str(&redis_config_str).unwrap();
    let url = redis_config["redis"]["url"].as_str().unwrap();
    let port: u16 = redis_config["redis"]["port"].as_integer().unwrap() as u16;

    let storage = RedisStorage::new("redis_storage_drop_column", url, port);
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE dummy (id INTEGER PRIMARY KEY, name TEXT);");
    exec!(glue r#"INSERT INTO dummy (id, name) values (1, 'Superman');"#);
    exec!(glue r#"INSERT INTO dummy (id, name) values (11, 'Batman');"#);
    exec!(glue "ALTER TABLE dummy DROP COLUMN name");

    let ret: Vec<Payload> = glue.execute("SELECT * FROM dummy;").await.unwrap();
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

    // TABLE dummy was dropped.
    // It should be able to create the same namespace again.
    let storage = RedisStorage::new("redis_storage_drop_column", url, port);
    let mut glue = Glue::new(storage);

    // Second test without PRIMARY KEY
    exec!(glue "CREATE TABLE dummy (id INTEGER, name TEXT);");
    exec!(glue r#"INSERT INTO dummy (id, name) values (1, 'Superman');"#);
    exec!(glue r#"INSERT INTO dummy (id, name) values (11, 'Batman');"#);
    exec!(glue "ALTER TABLE dummy DROP COLUMN name");

    let ret: Vec<Payload> = glue.execute("SELECT * FROM dummy;").await.unwrap();
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

#[tokio::test]
async fn redis_storage_alter_tablename() {
    let mut path = env::current_dir().unwrap();
    path.push("tests/redis-storage.toml");
    let redis_config_str = fs::read_to_string(path).unwrap();
    let redis_config: toml::Value = toml::from_str(&redis_config_str).unwrap();
    let url = redis_config["redis"]["url"].as_str().unwrap();
    let port: u16 = redis_config["redis"]["port"].as_integer().unwrap() as u16;

    let storage = RedisStorage::new("redis_storage_alter_tablename", url, port);
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE dummy (id INTEGER PRIMARY KEY);");
    exec!(glue "INSERT INTO dummy (id) values (1)");
    exec!(glue "INSERT INTO dummy (id) values (11)");
    exec!(glue "ALTER TABLE dummy RENAME TO dumdum");

    let ret = glue.execute("SELECT * FROM dummy;").await;
    assert!(ret.is_err());

    let ret: Vec<Payload> = glue.execute("SELECT * FROM dumdum;").await.unwrap();
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

    exec!(glue "DROP TABLE dumdum;");
}
