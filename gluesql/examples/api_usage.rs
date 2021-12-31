use {
    futures::executor::block_on,
    gluesql::{
        prelude::{execute, parse, translate, Glue},
        sled_storage::SledStorage,
    },
};

fn immutable_api() {
    let storage = SledStorage::new("data/immutable-api").unwrap();

    let sqls = "
        CREATE TABLE Glue (id INTEGER);
        INSERT INTO Glue VALUES (100);
        INSERT INTO Glue VALUES (200);
        DROP TABLE Glue;
    ";

    parse(sqls)
        .unwrap()
        .iter()
        .fold(storage, |storage, parsed| {
            let statement = translate(parsed).unwrap();
            let (storage, _) = block_on(execute(storage, &statement)).unwrap();

            storage
        });
}

fn mutable_api() {
    let storage = SledStorage::new("data/mutable-api").unwrap();
    let mut glue = Glue::new(storage);

    let sqls = [
        "CREATE TABLE Glue (id INTEGER);",
        "INSERT INTO Glue VALUES (100);",
        "INSERT INTO Glue VALUES (200);",
        "DROP TABLE Glue;",
    ];

    for sql in sqls {
        glue.execute(sql).unwrap();
    }
}

async fn async_mutable_api() {
    let storage = SledStorage::new("data/async-mutable-api").unwrap();
    let mut glue = Glue::new(storage);

    let sqls = [
        "CREATE TABLE Glue (id INTEGER);",
        "INSERT INTO Glue VALUES (100);",
        "INSERT INTO Glue VALUES (200);",
        "DROP TABLE Glue;",
    ];

    for sql in sqls {
        glue.execute_async(sql).await.unwrap();
    }
}

fn main() {
    mutable_api();
    immutable_api();
    block_on(async_mutable_api());
}
