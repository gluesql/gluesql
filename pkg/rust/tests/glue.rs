#![cfg(any(feature = "gluesql_memory_storage", feature = "sled-storage"))]
use {
    futures::executor::block_on,
    gluesql_core::{
        executor::Payload,
        prelude::{Glue, Value},
        store::{GStore, GStoreMut},
    },
};

async fn basic<T: GStore + GStoreMut>(mut glue: Glue<T>) {
    assert_eq!(
        glue.execute("DROP TABLE IF EXISTS api_test").await,
        Ok(vec![Payload::DropTable])
    );

    assert_eq!(
        glue.execute(
            "CREATE TABLE api_test (id INTEGER, name TEXT, nullable TEXT NULL, is BOOLEAN)"
        )
        .await,
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
        )
        .await,
        Ok(vec![Payload::Insert(2)])
    );

    assert_eq!(
        glue.execute("SELECT id, name, is FROM api_test").await,
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

#[cfg(feature = "sled-storage")]
#[test]
fn sled_basic() {
    use sled_storage::{sled, SledStorage};

    let config = sled::Config::default()
        .path("data/using_config")
        .temporary(true);

    let storage = SledStorage::try_from(config).unwrap();
    let glue = Glue::new(storage);

    block_on(basic(glue));
}

#[cfg(feature = "gluesql_memory_storage")]
#[test]
fn memory_basic() {
    use gluesql_memory_storage::MemoryStorage;

    let storage = MemoryStorage::default();
    let glue = Glue::new(storage);

    block_on(basic(glue));
}
