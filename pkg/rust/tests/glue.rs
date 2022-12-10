#![cfg(any(feature = "memory-storage", feature = "sled-storage"))]
use gluesql_core::{
    executor::Payload,
    prelude::{Glue, Value},
    store::{GStore, GStoreMut},
};

fn basic<T: GStore + GStoreMut>(mut glue: Glue<T>) {
    assert_eq!(
        glue.execute("DROP TABLE IF EXISTS api_test"),
        Ok(vec![Payload::DropTable])
    );

    assert_eq!(
        glue.execute(
            "CREATE TABLE api_test (id INTEGER, name TEXT, nullable TEXT NULL, is BOOLEAN)"
        ),
        Ok(vec![Payload::Create])
    );

    assert_eq!(
        glue.execute(
            "
                INSERT INTO
                    api_test (id, name, nullable, is)
                VALUES
                    (1, 'test1', 'not null', TRUE),
                    (2, 'test2', NULL, FALSE)"
        ),
        Ok(vec![Payload::Insert(2)])
    );

    assert_eq!(
        glue.execute("SELECT id, name, is FROM api_test"),
        Ok(vec![Payload::Select {
            labels: vec![String::from("id"), String::from("name"), String::from("is")],
            rows: vec![
                vec![
                    Value::I64(1),
                    Value::Str(String::from("test1")),
                    Value::Bool(true)
                ],
                vec![
                    Value::I64(2),
                    Value::Str(String::from("test2")),
                    Value::Bool(false)
                ],
            ]
        }])
    );
}

async fn basic_async<T: GStore + GStoreMut>(mut glue: Glue<T>) {
    assert_eq!(
        glue.execute_async("DROP TABLE IF EXISTS api_test").await,
        Ok(vec![Payload::DropTable])
    );

    assert_eq!(
        glue.execute_async(
            "CREATE TABLE api_test (id INTEGER, name TEXT, nullable TEXT NULL, is BOOLEAN)"
        )
        .await,
        Ok(vec![Payload::Create])
    );
}

#[cfg(feature = "sled-storage")]
#[test]
fn sled_basic() {
    use sled_storage::{sled, SledStorage};

    let config = sled::Config::default()
        .path("data/using_config")
        .temporary(true);

    let storage = SledStorage::try_from(config).unwrap();
    let glue = Glue::new(storage);

    basic(glue);
}

#[cfg(feature = "memory-storage")]
#[test]
fn memory_basic() {
    use memory_storage::MemoryStorage;

    let storage = MemoryStorage::default();
    let glue = Glue::new(storage);

    basic(glue);
}

#[cfg(feature = "memory-storage")]
#[test]
fn memory_basic_async() {
    use {futures::executor::block_on, memory_storage::MemoryStorage};

    let storage = MemoryStorage::default();
    let glue = Glue::new(storage);

    block_on(basic_async(glue));
}
