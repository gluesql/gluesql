#[cfg(feature = "sled-storage")]
use {
    gluesql::{sled, Glue, SledStorage},
    std::convert::TryFrom,
};

#[cfg(feature = "sled-storage")]
fn main() {
    let config = sled::Config::default()
        .path("data/using_config")
        .temporary(true)
        .mode(sled::Mode::HighThroughput);

    let storage = SledStorage::try_from(config).unwrap();

    let mut glue = Glue::new(storage);

    let sqls = "
        CREATE TABLE Glue (id INTEGER);
        INSERT INTO Glue VALUES (100);
        INSERT INTO Glue VALUES (200);
        DROP TABLE Glue;
    ";

    glue.execute(sqls).unwrap();
}

#[cfg(not(feature = "sled-storage"))]
fn main() {}
