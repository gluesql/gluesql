#[cfg(feature = "gluesql_memory_storage")]
mod parameter_binding_example {
    use gluesql::{
        gluesql_memory_storage::MemoryStorage,
        prelude::{Glue, Result},
    };

    pub async fn run() -> Result<()> {
        let storage = MemoryStorage::default();
        let mut glue = Glue::new(storage);

        glue.execute("DROP TABLE IF EXISTS bind_example").await?;

        glue.execute(
            "CREATE TABLE bind_example (
                id INTEGER,
                name TEXT
            )",
        )
        .await?;

        let first = gluesql::params![1_i64, "Alice"];
        glue.execute_with_params("INSERT INTO bind_example (id, name) VALUES ($1, $2)", first)
            .await?;

        let rows = glue
            .execute_with_params(
                "SELECT name FROM bind_example WHERE id = $1",
                gluesql::params![1_i64],
            )
            .await?;

        println!("query result: {rows:?}");

        Ok(())
    }
}

fn main() {
    #[cfg(feature = "gluesql_memory_storage")]
    {
        if let Err(error) = futures::executor::block_on(parameter_binding_example::run()) {
            eprintln!("error: {error}");
        }
    }
}
