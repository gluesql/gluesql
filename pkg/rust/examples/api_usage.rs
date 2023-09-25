#[cfg(feature = "sled-storage")]
mod api_usage {
    use gluesql::{prelude::Glue, storage::SledStorage};

    pub async fn run() {
        let storage = SledStorage::new("data/mutable-api").unwrap();
        let mut glue = Glue::new(storage);

        let sqls = [
            "CREATE TABLE Glue (id INTEGER);",
            "INSERT INTO Glue VALUES (100);",
            "INSERT INTO Glue VALUES (200);",
            "DROP TABLE Glue;",
        ];

        for sql in sqls {
            glue.execute(sql).await.unwrap();
        }
    }
}

fn main() {
    #[cfg(feature = "sled-storage")]
    futures::executor::block_on(api_usage::run());
}
