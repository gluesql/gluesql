#[cfg(feature = "gluesql_sled_storage")]
use {
    futures::executor::block_on,
    gluesql::{gluesql_sled_storage::SledStorage, prelude::Glue},
    gluesql_sled_storage::sled,
    std::convert::TryFrom,
};

fn main() {
    #[cfg(feature = "gluesql_sled_storage")]
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

        block_on(glue.execute(sqls)).unwrap();
    }
}
