#[cfg(feature = "sled-storage")]
use futures::executor::block_on;
#[cfg(feature = "sled-storage")]
use gluesql::{execute, parse, Glue, SledStorage};

#[cfg(feature = "sled-storage")]
fn immutable_api() {
    let storage = SledStorage::new("data/immutable-api").unwrap();

    let sqls = "
        CREATE TABLE Glue (id INTEGER);
        INSERT INTO Glue VALUES (100);
        INSERT INTO Glue VALUES (200);
        DROP TABLE Glue;
    ";

    parse(sqls).unwrap().iter().fold(storage, |storage, query| {
        let (storage, _) = block_on(execute(storage, query)).unwrap();

        storage
    });
}

#[cfg(feature = "sled-storage")]
fn mutable_api() {
    let storage = SledStorage::new("data/mutable-api").unwrap();
    let mut glue = Glue::new(storage);

    let sqls = "
        CREATE TABLE Glue (id INTEGER);
        INSERT INTO Glue VALUES (100);
        INSERT INTO Glue VALUES (200);
        DROP TABLE Glue;
    ";

    for query in parse(sqls).unwrap() {
        glue.execute(&query).unwrap();
    }
}

fn main() {
    #[cfg(feature = "sled-storage")]
    {
        mutable_api();
        immutable_api();
    }
}
