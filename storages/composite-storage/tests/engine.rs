use {
    gluesql_composite_storage::CompositeStorage,
    gluesql_core::{
        prelude::{
            Glue,
            Value::{I64, Null, Str},
        },
        store::Store,
    },
    gluesql_memory_storage::MemoryStorage,
    test_suite::*,
};

#[test]
fn engine_less_schema_uses_owning_storage() {
    let mut legacy = Glue::new(MemoryStorage::default());
    legacy.execute("CREATE TABLE Legacy (id INTEGER);").unwrap();
    legacy.execute("INSERT INTO Legacy VALUES (1);").unwrap();

    let mut storage = CompositeStorage::new();
    storage.push("DEFAULT", MemoryStorage::default());
    storage.push("LEGACY", legacy.storage);
    storage.set_default("DEFAULT");

    let mut glue = Glue::new(storage);
    let schema = glue.storage.fetch_schema("Legacy").unwrap().unwrap();
    assert_eq!(schema.engine.as_deref(), Some("LEGACY"));

    let schema = glue
        .storage
        .fetch_all_schemas()
        .unwrap()
        .into_iter()
        .find(|schema| schema.table_name == "Legacy")
        .unwrap();
    assert_eq!(schema.engine.as_deref(), Some("LEGACY"));

    glue.execute("ALTER TABLE Legacy ADD COLUMN name TEXT;")
        .unwrap();
    glue.execute("INSERT INTO Legacy VALUES (2, 'two');")
        .unwrap();

    assert_eq!(
        glue.execute("SELECT * FROM Legacy;")
            .unwrap()
            .into_iter()
            .next()
            .unwrap(),
        select_with_null!(
            id     | name;
            I64(1)   Null;
            I64(2)   Str("two".to_owned())
        )
    );
}
