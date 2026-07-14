#[cfg(feature = "gluesql_sled_storage")]
mod hello_query_builder {
    use {
        gluesql::{
            core::query_builder::{self, Execute},
            gluesql_sled_storage::SledStorage,
            prelude::{Glue, Payload, Value},
        },
        std::fs,
    };

    pub fn run() {
        /*
            Initiate a connection
        */
        /*
            Open a Sled database, this will create one if one does not yet exist
        */
        let sled_dir = "/tmp/gluesql/hello_query_builder";
        fs::remove_dir_all(sled_dir).unwrap_or(());
        let storage = SledStorage::new(sled_dir).expect("Something went wrong!");
        /*
            Wrap the Sled database with Glue
        */
        let mut glue = Glue::new(storage);

        /*
            Create table
        */
        query_builder::table("greet")
            .create_table()
            .add_column("name TEXT")
            .execute(&mut glue)
            .expect("Execution failed");

        /*
            Insert a row
        */
        query_builder::table("greet")
            .insert()
            .values(vec!["'Query Builder'"])
            .execute(&mut glue)
            .expect("Execution failed");

        /*
            Select inserted row
        */
        let result = query_builder::table("greet")
            .select()
            .project("name")
            .execute(&mut glue)
            .expect("Failed to execute");

        /*
            Query results are wrapped into a payload enum, on the basis of the query type
        */
        let Payload::Select { labels: _, rows } = result else {
            panic!("Unexpected result: {result:?}")
        };

        let first_row = &rows[0];
        let first_value = first_row.iter().next().unwrap();

        /*
            Row values are wrapped into a value enum, on the basis of the result type
        */
        let to_greet = match first_value {
            Value::Str(to_greet) => to_greet,
            value => panic!("Unexpected type: {value:?}"),
        };

        println!("Hello {to_greet}!"); // Will always output "Hello Query Builder!"
    }
}

fn main() {
    #[cfg(feature = "gluesql_sled_storage")]
    hello_query_builder::run();
}
