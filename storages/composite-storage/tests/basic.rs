use {
    gluesql_composite_storage::CompositeStorage,
    gluesql_core::{
        error::FetchError,
        prelude::{Error, Glue, Value::I64},
    },
    gluesql_memory_storage::MemoryStorage,
    test_suite::*,
};

#[tokio::test]
async fn basic() {
    let m1 = MemoryStorage::default();
    let m2 = MemoryStorage::default();

    let mut storage = CompositeStorage::new();
    storage.push("M1", m1);
    storage.push("M2", m2);

    let mut glue = Glue::new(storage);

    glue.storage.set_default("M1");
    glue.execute("CREATE TABLE Foo (id INTEGER);")
        .await
        .unwrap();

    glue.storage.set_default("M2");
    glue.execute("CREATE TABLE Bar (id INTEGER);")
        .await
        .unwrap();

    glue.execute("INSERT INTO Foo VALUES (1), (2);")
        .await
        .unwrap();
    glue.execute("INSERT INTO Bar VALUES (5), (7);")
        .await
        .unwrap();

    assert_eq!(
        glue.execute(
            "SELECT
                Foo.id AS fid,
                Bar.id AS bid
            FROM Foo
            JOIN Bar;
        "
        )
        .await
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
        glue.execute("SELECT * FROM Bar;").await,
        Err(FetchError::TableNotFound("Bar".to_owned()).into())
    );

    glue.storage.set_default("M1");
    glue.storage.remove_default();
    assert_eq!(
        glue.execute("CREATE TABLE Tae (id INTEGER);").await,
        Err(Error::StorageMsg(
            "storage not found for table: Tae".to_owned()
        ))
    );

    glue.storage.clear();
    assert_eq!(
        glue.execute("SELECT * FROM Foo;").await,
        Err(FetchError::TableNotFound("Foo".to_owned()).into())
    );
}
