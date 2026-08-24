#[cfg(feature = "gluesql-redb-storage")]
mod hello_world {
    use {
        gluesql::{
            FromGlueRow,
            gluesql_redb_storage::RedbStorage,
            prelude::{Glue, SelectExt},
        },
        std::fs,
    };

    #[derive(Debug, FromGlueRow)]
    struct GreetRow {
        name: String,
    }

    pub fn run() {
        /*
            Initiate a connection
        */
        /*
            Open a Redb database, this will create one if one does not yet exist
        */
        let redb_dir = "/tmp/gluesql";
        let redb_path = format!("{redb_dir}/hello_world");
        fs::create_dir_all(redb_dir).unwrap();
        fs::remove_file(&redb_path).unwrap_or(());
        let storage = RedbStorage::new(redb_path).expect("Something went wrong!");
        /*
            Wrap the Redb database with Glue
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

        glue.execute(queries).expect("Execution failed");

        /*
            Select inserted row
        */
        let queries = "
            SELECT name FROM greet
        ";

        let result = glue.execute(queries).expect("Failed to execute");

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
    #[cfg(feature = "gluesql-redb-storage")]
    hello_world::run();
}
