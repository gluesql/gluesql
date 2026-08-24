use {
    gluesql_composite_storage::CompositeStorage,
    gluesql_core::prelude::{Error, Glue, Value::I64},
    gluesql_memory_storage::MemoryStorage,
    gluesql_redb_storage::RedbStorage,
    std::fs::{create_dir_all, remove_file},
    test_suite::*,
};

#[test]
fn memory_and_redb() {
    let memory_storage = MemoryStorage::default();
    let redb_storage = {
        let _ = create_dir_all("data");
        let path = "data/memory_and_redb";
        let _ = remove_file(path);

        RedbStorage::new(path).unwrap()
    };

    let mut storage = CompositeStorage::new();
    storage.push("MEMORY", memory_storage);
    storage.push("REDB", redb_storage);
    storage.set_default("MEMORY");

    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Foo (foo_id INTEGER) ENGINE = MEMORY;")
        .unwrap();
    glue.execute("CREATE TABLE Bar (bar_id INTEGER, foo_id INTEGER) ENGINE = REDB;")
        .unwrap();

    glue.execute("INSERT INTO Foo VALUES (1), (2), (3), (4), (5);")
        .unwrap();
    glue.execute("INSERT INTO Bar VALUES (10, 1), (20, 3), (30, 3), (40, 3), (50, 5);")
        .unwrap();

    assert_eq!(
        glue.execute("SELECT Bar.* FROM Bar LEFT JOIN Foo ON Bar.foo_id = Foo.foo_id;")
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
        glue.execute("BEGIN;").unwrap_err(),
        Error::StorageMsg("[CompositeStorage] Transaction::begin is not supported".to_owned()),
    );
}
