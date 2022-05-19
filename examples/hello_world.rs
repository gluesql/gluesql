#[cfg(feature = "sled-storage")]
mod hello_world {
    use gluesql::{
        prelude::{Glue, Payload, Value},
        sled_storage::SledStorage,
    };
    use std::fs;

    pub fn run() {
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
          INSERT INTO greet VALUES (\"World\");
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
        let rows = match &result[0] {
            Payload::Select { labels: _, rows } => rows,
            _ => panic!("Unexpected result: {:?}", result),
        };

        let first_row = &rows[0];
        let first_value = &first_row[0];

        /*
            Row values are wrapped into a value enum, on the basis of the result type
        */
        let to_greet = match first_value {
            Value::Str(to_greet) => to_greet,
            value => panic!("Unexpected type: {:?}", value),
        };

        println!("Hello {}!", to_greet); // Will always output "Hello World!"
    }
}

fn main() {
    #[cfg(feature = "sled-storage")]
    hello_world::run();
}
