#[cfg(feature = "parquet-storage")]
mod api_usage {
    use gluesql::{parquet_storage::ParquetStorage, prelude::Glue};

    pub async fn run() {
        let path = "./";
        let parquet_storage = ParquetStorage::new(path).unwrap();
        let mut glue = Glue::new(parquet_storage);
        glue.execute("CREATE TABLE food (name TEXT);")
            .await
            .unwrap();

        glue.execute("INSERT INTO food VALUES('sushi'), ('steak');")
            .await
            .unwrap();

        glue.execute("UPDATE food SET name = 'Nigiri Sushi' WHERE name='sushi';")
            .await
            .unwrap();

        glue.execute("DELETE name FROM food WHERE name = 'steak';")
            .await
            .unwrap();

        glue.execute("SELECT * FROM food;").await.unwrap();
    }
}

fn main() {
    #[cfg(feature = "parquet-storage")]
    futures::executor::block_on(api_usage::run());
}
