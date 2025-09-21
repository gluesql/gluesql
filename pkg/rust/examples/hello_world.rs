#[cfg(feature = "gluesql_sled_storage")]
mod hello_world {
    use {
        gluesql::{
            FromGlueRow,
            gluesql_sled_storage::SledStorage,
            prelude::{Glue, SelectExt},
        },
        std::fs,
    };

    #[derive(Debug, FromGlueRow)]
    struct GreetRow {
        name: String,
    }

    pub async fn run() {
        /*
            Initiate a connection
        */
        /*
            Open a Sled database, this will create one if one does not yet exist
        */
        let sled_dir = "/tmp/gluesql/hello_world";
        fs::remove_dir_all(sled_dir).unwrap_or(());
        let storage = SledStorage::new(sled_dir).expect("Something went wrong!");
        /*
            Wrap the Sled database with Glue
        */
        let mut glue = Glue::new(storage);

        /*
            Create table then insert a row

            Write queries as a string
        */
        let queries = "
            CREATE TABLE greet (name TEXT);
            INSERT INTO greet VALUES ('World');
        ";

        glue.execute(queries).await.expect("Execution failed");

        /*
            Select inserted row
        */
        let queries = "
            SELECT name FROM greet
        ";

        let result = glue.execute(queries).await.expect("Failed to execute");

        /*
            Query results are wrapped into a payload enum, on the basis of the query type
        */
        assert_eq!(result.len(), 1);

        let rows = result
            .rows_as::<GreetRow>()
            .expect("Failed to decode select rows");

        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0].name, "World");

        println!("Hello {}!", rows[0].name); // Will always output "Hello World!"
    }
}

fn main() {
    #[cfg(feature = "gluesql_sled_storage")]
    futures::executor::block_on(hello_world::run());
}
