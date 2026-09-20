#![cfg(any(feature = "gluesql_memory_storage", feature = "gluesql-redb-storage"))]

use gluesql::{
    FromGlueRow,
    core::store::{GStore, GStoreMut, Planner},
    prelude::*,
};

/// Exercises the public API surface: DDL, DML, SELECT, and `FromGlueRow` conversion.
fn basic<T: GStore + GStoreMut + Planner>(glue: &mut Glue<T>) {
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

/// Verifies that statements in a single `execute` call are planned against the schema
/// left behind by the statements before them (#2009).
fn batch<T: GStore + GStoreMut + Planner>(glue: &mut Glue<T>) {
    use gluesql::core::planner::PlannerError;

    // a table created earlier in the same call must be visible to the planner
    assert_eq!(
        glue.execute(
            "DROP TABLE IF EXISTS batch_doc;
             CREATE TABLE batch_doc;
             INSERT INTO batch_doc VALUES ('{\"a\": 1}');
             SELECT a FROM batch_doc;"
        ),
        Ok(vec![
            Payload::DropTable(0),
            Payload::Create,
            Payload::Insert(1),
            Payload::Select {
                labels: vec!["a".to_owned()],
                rows: vec![vec![Value::I64(1)]],
            },
        ])
    );

    // validation must see schemas created earlier in the same call
    assert_eq!(
        glue.execute(
            "DROP TABLE IF EXISTS batch_x;
             DROP TABLE IF EXISTS batch_y;
             CREATE TABLE batch_x (id INTEGER);
             CREATE TABLE batch_y (id INTEGER);
             SELECT id FROM batch_x JOIN batch_y ON batch_x.id = batch_y.id;"
        ),
        Err(PlannerError::ColumnReferenceAmbiguous("id".to_owned()).into())
    );
}

#[cfg(feature = "gluesql-redb-storage")]
#[test]
fn redb() {
    use {
        gluesql_redb_storage::RedbStorage,
        std::fs::{create_dir_all, remove_file},
    };

    let _ = create_dir_all("data");
    let path = "data/redb_basic";
    let _ = remove_file(path);

    let storage = RedbStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    basic(&mut glue);
    batch(&mut glue);
}

#[cfg(feature = "gluesql_memory_storage")]
#[test]
fn memory() {
    use gluesql_memory_storage::MemoryStorage;

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    basic(&mut glue);
    batch(&mut glue);
}
