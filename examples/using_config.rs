#[cfg(sled_storage)]
use {
    gluesql::{prelude::Glue, sled_storage::SledStorage},
    sled_storage::sled,
    std::convert::TryFrom,
};

fn main() {
    #[cfg(sled_storage)]
    {
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
}
