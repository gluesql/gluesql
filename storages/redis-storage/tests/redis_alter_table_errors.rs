#![cfg(feature = "test-redis")]

use {
    gluesql_core::{
        data::{Key, Value},
        error::{AlterTableError, Error},
        prelude::Glue,
        store::DataRow,
    },
    gluesql_redis_storage::RedisStorage,
    redis, serde_json,
    std::{collections::HashMap, env, fs},
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

#[tokio::test]
async fn redis_storage_alter_table_errors() {
    let mut path = env::current_dir().unwrap();
    path.push("tests/redis-storage.toml");
    let redis_config_str = fs::read_to_string(path).unwrap();
    let redis_config: toml::Value = toml::from_str(&redis_config_str).unwrap();
    let url = redis_config["redis"]["url"].as_str().unwrap();
    let port: u16 = redis_config["redis"]["port"].as_integer().unwrap() as u16;

    let storage = RedisStorage::new("redis_storage_alter_table_errors", url, port);
    let mut glue = Glue::new(storage);

    exec!(glue "DROP TABLE IF EXISTS dummy;");

    test!(
        glue
        "ALTER TABLE not_exists RENAME TO something;",
        Err(AlterTableError::TableNotFound("not_exists".to_owned()).into())
    );

    exec!(glue "CREATE TABLE dummy (id INTEGER PRIMARY KEY, name TEXT);");
    exec!(glue "INSERT INTO dummy (id, name) VALUES (1, 'a');");

    let client = redis::Client::open(format!("redis://{}:{}", url, port)).unwrap();
    let mut conn = client.get_connection().unwrap();
    let key = format!(
        "{}#dummy#{}",
        "redis_storage_alter_table_errors",
        serde_json::to_string(&Key::I64(1)).unwrap()
    );

    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("oops")
        .query(&mut conn)
        .unwrap();

    test!(
        glue
        "ALTER TABLE dummy ADD COLUMN age INT;",
        Err(Error::StorageMsg(
            "[RedisStorage] failed to deserialize value=oops error=expected value at line 1 column 1".to_owned()
        ))
    );

    let row_vec = DataRow::Vec(vec![Value::I64(1), Value::Str("a".to_owned())]);
    let value_vec = serde_json::to_string(&row_vec).unwrap();
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg(&value_vec)
        .query(&mut conn)
        .unwrap();

    let mut map = HashMap::new();
    map.insert("id".to_owned(), Value::I64(1));
    map.insert("name".to_owned(), Value::Str("a".to_owned()));
    let row_map = DataRow::Map(map);
    let value_map = serde_json::to_string(&row_map).unwrap();
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg(&value_map)
        .query(&mut conn)
        .unwrap();

    test!(
        glue
        "ALTER TABLE dummy ADD COLUMN age INT;",
        Err(Error::StorageMsg(
            "[RedisStorage] conflict - add_column failed: schemaless row found".to_owned()
        ))
    );

    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg(&value_vec)
        .query(&mut conn)
        .unwrap();

    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("oops")
        .query(&mut conn)
        .unwrap();

    test!(
        glue
        "ALTER TABLE dummy DROP COLUMN name;",
        Err(Error::StorageMsg(
            "[RedisStorage] failed to deserialize value=oops error=expected value at line 1 column 1".to_owned()
        ))
    );

    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg(&value_map)
        .query(&mut conn)
        .unwrap();

    test!(
        glue
        "ALTER TABLE dummy DROP COLUMN name;",
        Err(Error::StorageMsg(
            "[RedisStorage] conflict - add_column failed: schemaless row found".to_owned()
        ))
    );

    exec!(glue "DROP TABLE dummy;");
}
