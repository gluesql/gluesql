use {
    gluesql_composite_storage::CompositeStorage,
    gluesql_core::prelude::{Glue, Value::I64},
    gluesql_json_storage::JsonStorage,
    gluesql_memory_storage::MemoryStorage,
    std::fs::remove_dir_all,
    test_suite::*,
};

#[test]
fn memory_and_json() {
    let path = "tmp/memory_and_json";
    let _ = remove_dir_all(path);
    let json_storage = JsonStorage::new(path).expect("JsonStorage::new");

    let mut storage = CompositeStorage::new();
    storage.push("MEMORY", MemoryStorage::default());
    storage.push("JSON", json_storage);
    storage.set_default("MEMORY");

    let mut glue = Glue::new(storage);
    glue.execute("CREATE TABLE Bar (id INTEGER) ENGINE = JSON;")
        .unwrap();
    glue.execute("INSERT INTO Bar VALUES (1);").unwrap();

    assert_eq!(
        glue.execute("SELECT * FROM Bar;")
            .unwrap()
            .into_iter()
            .next()
            .unwrap(),
        select!(id I64; 1),
    );

    drop(glue);
    remove_dir_all(path).unwrap();
}
