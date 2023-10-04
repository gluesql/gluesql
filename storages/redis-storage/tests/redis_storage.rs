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
generate_store_tests!(tokio::test, RedisStorageTester);

generate_alter_table_tests!(tokio::test, RedisStorageTester);

generate_metadata_table_tests!(tokio::test, RedisStorageTester);

generate_custom_function_tests!(tokio::test, RedisStorageTester);

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).await.unwrap();
    };
}

/// check if it's able to create tables with same name in different namespace
#[tokio::test]
async fn redis_storage_namespace() {
    use gluesql_core::prelude::Glue;
    let url = "localhost";
    let port: u16 = 6379;

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
    use chrono::NaiveDate;
    use gluesql_core::prelude::{Glue, Payload, Value};

    let url = "localhost";
    let port: u16 = 6379;

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
            assert_eq!(rows[0][1], Value::Str("Superman".to_string()));
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
                if r[1] == Value::Str("Batman".to_string()) {
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
                assert_ne!(r[1], Value::Str("Super".to_string()));
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
    use chrono::NaiveDate;
    use gluesql_core::prelude::{Glue, Payload, Value};

    let url = "localhost";
    let port: u16 = 6379;

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

            assert_eq!(rows[0][0], Value::Str("Batman".to_string()));
            assert_eq!(
                rows[0][1],
                Value::Date(NaiveDate::from_ymd_opt(2011, 12, 31).unwrap())
            );
        }
        _ => unreachable!(),
    }

    exec!(glue "DROP TABLE Heroes;");
}

#[tokio::test]
async fn redis_storage_tables() {
    use chrono::NaiveDate;

    let url = "localhost";
    let port: u16 = 6379;
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
            assert_eq!(rows[0][1], Value::Str("Wanda".to_string()));
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
            assert_eq!(rows[0][1], Value::Str("Superman".to_string()));
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
    use gluesql_core::prelude::Glue;

    let url = "localhost";
    let port: u16 = 6379;
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
    use gluesql_core::prelude::Glue;

    let url = "localhost";
    let port: u16 = 6379;
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
    use gluesql_core::prelude::Glue;

    let url = "localhost";
    let port: u16 = 6379;
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

        println!("disconnect");
    }

    {
        println!("reconnect");
        let url = "localhost";
        let port: u16 = 6379;
        let storage = RedisStorage::new("redis_storage_reconnect", url, port);
        let mut glue = Glue::new(storage);

        println!("do SELECT");
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

        // TODO: check dummy is removed
    }
}
