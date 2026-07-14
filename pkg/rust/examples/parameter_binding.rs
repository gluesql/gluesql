#[cfg(feature = "gluesql_memory_storage")]
mod parameter_binding_example {
    use gluesql::{
        gluesql_memory_storage::MemoryStorage,
        prelude::{Glue, Result},
    };

    pub fn run() -> Result<()> {
        let storage = MemoryStorage::default();
        let mut glue = Glue::new(storage);

        glue.execute("DROP TABLE IF EXISTS bind_example")?;

        glue.execute(
            "CREATE TABLE bind_example (
                id INTEGER,
                name TEXT
            )",
        )?;

        let first = gluesql::params![1_i64, "Alice"];
        glue.execute_with_params("INSERT INTO bind_example (id, name) VALUES ($1, $2)", first)?;

        let rows = glue.execute_with_params(
            "SELECT name FROM bind_example WHERE id = $1",
            gluesql::params![1_i64],
        )?;

        println!("query result: {rows:?}");

        Ok(())
    }
}

fn main() {
    #[cfg(feature = "gluesql_memory_storage")]
    {
        if let Err(error) = parameter_binding_example::run() {
            eprintln!("error: {error}");
        }
    }
}
