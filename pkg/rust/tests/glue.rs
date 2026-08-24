#![cfg(any(feature = "gluesql_memory_storage", feature = "gluesql-redb-storage"))]

use gluesql::{
    FromGlueRow,
    core::store::{GStore, GStoreMut, Planner},
    prelude::*,
};

fn basic<T: GStore + GStoreMut + Planner>(mut glue: Glue<T>) {
    // Demonstrate FromGlueRow derive + Payload conversion to struct
    #[derive(Debug, PartialEq, FromGlueRow)]
    struct ApiRow {
        id: i64,
        name: String,
        is: bool,
    }

    assert_eq!(
        glue.execute("DROP TABLE IF EXISTS api_test"),
        Ok(vec![Payload::DropTable(0)])
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

    let rows: Vec<ApiRow> = glue
        .execute("SELECT id, name, is FROM api_test")
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

#[cfg(feature = "gluesql-redb-storage")]
#[test]
fn redb_basic() {
    use {
        gluesql_redb_storage::RedbStorage,
        std::fs::{create_dir_all, remove_file},
    };

    let _ = create_dir_all("data");
    let path = "data/redb_basic";
    let _ = remove_file(path);

    let storage = RedbStorage::new(path).unwrap();
    let glue = Glue::new(storage);

    basic(glue);
}

#[cfg(feature = "gluesql_memory_storage")]
#[test]
fn memory_basic() {
    use gluesql_memory_storage::MemoryStorage;

    let storage = MemoryStorage::default();
    let glue = Glue::new(storage);

    basic(glue);
}
