use {
    gluesql_composite_storage::CompositeStorage,
    gluesql_core::{
        executor::FetchError,
        prelude::{Glue, Value::I64},
        result::Error,
    },
    gluesql_memory_storage::MemoryStorage,
    test_suite::*,
};

#[test]
fn basic() {
    let m1 = MemoryStorage::default();
    let m2 = MemoryStorage::default();

    let mut storage = CompositeStorage::new();
    storage.push("M1", m1);
    storage.push("M2", m2);

    let mut glue = Glue::new(storage);

    glue.storage.set_default("M1");
    glue.execute("CREATE TABLE Foo (id INTEGER);").unwrap();

    glue.storage.set_default("M2");
    glue.execute("CREATE TABLE Bar (id INTEGER);").unwrap();

    glue.execute("INSERT INTO Foo VALUES (1), (2);").unwrap();
    glue.execute("INSERT INTO Bar VALUES (5), (7);").unwrap();

    assert_eq!(
        glue.execute(
            "SELECT
                Foo.id AS fid,
                Bar.id AS bid
            FROM Foo
            JOIN Bar;
        "
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap(),
        select!(
            fid | bid;
            I64 | I64;
            1     5;
            1     7;
            2     5;
            2     7
        )
    );

    glue.storage.remove("M2");
    assert_eq!(
        glue.execute("SELECT * FROM Bar;"),
        Err(FetchError::TableNotFound("Bar".to_owned()).into())
    );

    glue.storage.set_default("M1");
    glue.storage.remove_default();
    assert_eq!(
        glue.execute("CREATE TABLE Tae (id INTEGER);"),
        Err(Error::StorageMsg(
            "storage not found for table: Tae".to_owned()
        ))
    );

    glue.storage.clear();
    assert_eq!(
        glue.execute("SELECT * FROM Foo;"),
        Err(FetchError::TableNotFound("Foo".to_owned()).into())
    );
}
