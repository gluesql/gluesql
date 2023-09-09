#[cfg(feature = "sled-storage")]
mod hello_ast_builder {
    use {
        gluesql::{
            core::ast_builder::{self, Execute},
            prelude::{Glue, Payload, Value},
            sled_storage::SledStorage,
        },
        std::fs,
    };

    pub async fn run() {
        /*
            Initiate a connection
        */
        /*
            Open a Sled database, this will create one if one does not yet exist
        */
        let sled_dir = "/tmp/gluesql/hello_ast_builder";
        fs::remove_dir_all(sled_dir).unwrap_or(());
        let storage = SledStorage::new(sled_dir).expect("Something went wrong!");
        /*
            Wrap the Sled database with Glue
        */
        let mut glue = Glue::new(storage);

        /*
            Create table
        */
        ast_builder::table("greet")
            .create_table()
            .add_column("name TEXT")
            .execute(&mut glue)
            .await
            .expect("Execution failed");

        /*
            Insert a row
        */
        ast_builder::table("greet")
            .insert()
            .values(vec!["'AST Builder'"])
            .execute(&mut glue)
            .await
            .expect("Execution failed");

        /*
            Select inserted row
        */
        let result = ast_builder::table("greet")
            .select()
            .project("name")
            .execute(&mut glue)
            .await
            .expect("Failed to execute");

        /*
            Query results are wrapped into a payload enum, on the basis of the query type
        */
        let rows = match result {
            Payload::Select { labels: _, rows } => rows,
            _ => panic!("Unexpected result: {:?}", result),
        };

        let first_row = &rows[0];
        let first_value = first_row.iter().next().unwrap();

        /*
            Row values are wrapped into a value enum, on the basis of the result type
        */
        let to_greet = match first_value {
            Value::Str(to_greet) => to_greet,
            value => panic!("Unexpected type: {:?}", value),
        };

        println!("Hello {}!", to_greet); // Will always output "Hello AST Builder!"
    }
}

fn main() {
    #[cfg(feature = "sled-storage")]
    futures::executor::block_on(hello_ast_builder::run());
}
