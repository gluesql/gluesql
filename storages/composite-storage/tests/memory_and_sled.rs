use {
    gluesql_composite_storage::CompositeStorage,
    gluesql_core::prelude::{Error, Glue, Value::I64},
    gluesql_memory_storage::MemoryStorage,
    gluesql_sled_storage::SledStorage,
    std::fs,
    test_suite::*,
};

#[tokio::test]
async fn memory_and_sled() {
    let memory_storage = MemoryStorage::default();
    let sled_storage = {
        let path = "data/memory_and_sled";
        fs::remove_dir_all(path).unwrap_or(());

        SledStorage::new(path).unwrap()
    };

    let mut storage = CompositeStorage::new();
    storage.push("MEMORY", memory_storage);
    storage.push("SLED", sled_storage);
    storage.set_default("MEMORY");

    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Foo (foo_id INTEGER) ENGINE = MEMORY;")
        .await
        .unwrap();
    glue.execute("CREATE TABLE Bar (bar_id INTEGER, foo_id INTEGER) ENGINE = SLED;")
        .await
        .unwrap();

    glue.execute("INSERT INTO Foo VALUES (1), (2), (3), (4), (5);")
        .await
        .unwrap();
    glue.execute("INSERT INTO Bar VALUES (10, 1), (20, 3), (30, 3), (40, 3), (50, 5);")
        .await
        .unwrap();

    assert_eq!(
        glue.execute("SELECT Bar.* FROM Bar LEFT JOIN Foo ON Bar.foo_id = Foo.foo_id;")
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap(),
        select!(
            bar_id | foo_id
            I64    | I64;
            10       1;
            20       3;
            30       3;
            40       3;
            50       5
        )
    );

    assert_eq!(
        glue.execute("BEGIN;").await.unwrap_err(),
        Error::StorageMsg("[CompositeStorage] Transaction::begin is not supported".to_owned()),
    );
}
