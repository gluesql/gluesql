#![cfg(feature = "test-redis")]

use {
    gluesql_core::{
        ast::{ColumnDef, DataType},
        data::{Key, Value},
        error::Error,
        prelude::Glue,
        store::AlterTable,
    },
    gluesql_redis_storage::RedisStorage,
    std::{collections::BTreeMap, env, fs},
};

fn get_config() -> (String, u16) {
    let mut path = env::current_dir().unwrap();
    path.push("tests/redis-storage.toml");
    let config_str = fs::read_to_string(path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let url = config["redis"]["url"].as_str().unwrap().to_owned();
    let port = config["redis"]["port"].as_integer().unwrap() as u16;
    (url, port)
}

#[tokio::test]
async fn add_column_non_vec_row_error() {
    let (url, port) = get_config();
    let storage = RedisStorage::new("redis_alter_table_non_vec", &url, port);
    let mut glue = Glue::new(storage);

    glue.execute("DROP TABLE IF EXISTS dummy;").await.unwrap();
    glue.execute("CREATE TABLE dummy (id INTEGER);")
        .await
        .unwrap();
    glue.execute("INSERT INTO dummy (id) VALUES (1);")
        .await
        .unwrap();

    let key = format!(
        "{}#{}#{}",
        "redis_alter_table_non_vec",
        "dummy",
        serde_json::to_string(&Key::I64(1)).unwrap()
    );
    let mut map = BTreeMap::new();
    map.insert("id".to_owned(), Value::I64(1));
    let value = serde_json::to_string(&map).unwrap();
    redis::cmd("SET")
        .arg(&key)
        .arg(value)
        .query::<()>(&mut *glue.storage.conn.lock().unwrap())
        .unwrap();

    let column_def = ColumnDef {
        name: "newcol".to_owned(),
        data_type: DataType::Int,
        nullable: true,
        default: None,
        unique: None,
        comment: None,
    };

    let result = glue.storage.add_column("dummy", &column_def).await;
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            r#"[RedisStorage] failed to deserialize value={"id":{"I64":1}} error=invalid type: map, expected a sequence at line 1 column 0"#.to_owned(),
        )),
    );
}

#[tokio::test]
async fn add_column_deserialize_error() {
    let (url, port) = get_config();
    let storage = RedisStorage::new("redis_alter_table_bad_row", &url, port);
    let mut glue = Glue::new(storage);

    glue.execute("DROP TABLE IF EXISTS dummy;").await.unwrap();
    glue.execute("CREATE TABLE dummy (id INTEGER);")
        .await
        .unwrap();
    glue.execute("INSERT INTO dummy (id) VALUES (1);")
        .await
        .unwrap();

    let key = format!(
        "{}#{}#{}",
        "redis_alter_table_bad_row",
        "dummy",
        serde_json::to_string(&Key::I64(1)).unwrap()
    );
    redis::cmd("SET")
        .arg(&key)
        .arg("not-json")
        .query::<()>(&mut *glue.storage.conn.lock().unwrap())
        .unwrap();

    let column_def = ColumnDef {
        name: "newcol".to_owned(),
        data_type: DataType::Int,
        nullable: true,
        default: None,
        unique: None,
        comment: None,
    };

    let result = glue.storage.add_column("dummy", &column_def).await;
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "[RedisStorage] failed to deserialize value=not-json error=expected ident at line 1 column 2".to_owned(),
        )),
    );
}

#[tokio::test]
async fn drop_column_non_vec_row_error() {
    let (url, port) = get_config();
    let storage = RedisStorage::new("redis_drop_column_non_vec", &url, port);
    let mut glue = Glue::new(storage);

    glue.execute("DROP TABLE IF EXISTS dummy;").await.unwrap();
    glue.execute("CREATE TABLE dummy (id INTEGER, foo INTEGER);")
        .await
        .unwrap();
    glue.execute("INSERT INTO dummy (id, foo) VALUES (1, 10);")
        .await
        .unwrap();

    let key = format!(
        "{}#{}#{}",
        "redis_drop_column_non_vec",
        "dummy",
        serde_json::to_string(&Key::I64(1)).unwrap()
    );
    let mut map = BTreeMap::new();
    map.insert("id".to_owned(), Value::I64(1));
    map.insert("foo".to_owned(), Value::I64(10));
    let value = serde_json::to_string(&map).unwrap();
    redis::cmd("SET")
        .arg(&key)
        .arg(value)
        .query::<()>(&mut *glue.storage.conn.lock().unwrap())
        .unwrap();

    let result = glue.storage.drop_column("dummy", "foo", false).await;
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            r#"[RedisStorage] failed to deserialize value={"foo":{"I64":10},"id":{"I64":1}} error=invalid type: map, expected a sequence at line 1 column 0"#.to_owned(),
        )),
    );
}

#[tokio::test]
async fn drop_column_deserialize_error() {
    let (url, port) = get_config();
    let storage = RedisStorage::new("redis_drop_column_bad_row", &url, port);
    let mut glue = Glue::new(storage);

    glue.execute("DROP TABLE IF EXISTS dummy;").await.unwrap();
    glue.execute("CREATE TABLE dummy (id INTEGER, foo INTEGER);")
        .await
        .unwrap();
    glue.execute("INSERT INTO dummy (id, foo) VALUES (1, 10);")
        .await
        .unwrap();

    let key = format!(
        "{}#{}#{}",
        "redis_drop_column_bad_row",
        "dummy",
        serde_json::to_string(&Key::I64(1)).unwrap()
    );
    redis::cmd("SET")
        .arg(&key)
        .arg("not-json")
        .query::<()>(&mut *glue.storage.conn.lock().unwrap())
        .unwrap();

    let result = glue.storage.drop_column("dummy", "foo", false).await;
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "[RedisStorage] failed to deserialize value=not-json error=expected ident at line 1 column 2".to_owned(),
        )),
    );
}

#[tokio::test]
async fn drop_column_short_row_error() {
    let (url, port) = get_config();
    let storage = RedisStorage::new("redis_drop_column_short_row", &url, port);
    let mut glue = Glue::new(storage);

    glue.execute("DROP TABLE IF EXISTS dummy;").await.unwrap();
    glue.execute("CREATE TABLE dummy (id INTEGER, foo INTEGER);")
        .await
        .unwrap();
    glue.execute("INSERT INTO dummy (id, foo) VALUES (1, 10);")
        .await
        .unwrap();

    let key = glue
        .storage
        .redis_execute_scan("dummy")
        .unwrap()
        .into_iter()
        .next()
        .expect("row key for dummy");
    let value = serde_json::to_string(&vec![Value::I64(1)]).unwrap();
    redis::cmd("SET")
        .arg(&key)
        .arg(value)
        .query::<()>(&mut *glue.storage.conn.lock().unwrap())
        .unwrap();

    let result = glue.storage.drop_column("dummy", "foo", false).await;
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            "[RedisStorage] conflict - drop_column failed: row too short for column index row_len=1 column_index=1".to_owned(),
        )),
    );
}
