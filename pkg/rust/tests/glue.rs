#![cfg(any(feature = "gluesql_memory_storage", feature = "gluesql_sled_storage"))]
use {
    futures::executor::block_on,
    gluesql::{FromGlueRow, core::row_conversion::SelectResultExt},
    gluesql_core::{
        executor::Payload,
        prelude::Glue,
        store::{GStore, GStoreMut},
    },
};

async fn basic<T: GStore + GStoreMut>(mut glue: Glue<T>) {
    assert_eq!(
        glue.execute("DROP TABLE IF EXISTS api_test").await,
        Ok(vec![Payload::DropTable(0)])
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

    // Demonstrate FromGlueRow derive + Payload conversion to struct
    #[derive(Debug, PartialEq, FromGlueRow)]
    struct ApiRow {
        id: i64,
        name: String,
        is: bool,
    }

    let rows: Vec<ApiRow> = glue
        .execute("SELECT id, name, is FROM api_test")
        .await
        .rows_as::<ApiRow>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ApiRow {
                id: 1,
                name: "test1".into(),
                is: true
            },
            ApiRow {
                id: 2,
                name: "test2".into(),
                is: false
            },
        ]
    );
}

#[cfg(feature = "gluesql_sled_storage")]
#[test]
fn sled_basic() {
    use gluesql_sled_storage::{SledStorage, sled};

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
