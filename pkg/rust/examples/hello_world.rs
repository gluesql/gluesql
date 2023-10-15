#[cfg(feature = "sled-storage")]
mod hello_world {
    use {
        gluesql::{prelude::Glue, sled_storage::SledStorage},
        std::fs,
    };

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

        let mut result = glue.execute(queries).await.expect("Failed to execute");

        /*
            Query results are wrapped into a payload enum, on the basis of the query type
        */
        assert_eq!(result.len(), 1);

        let payload = result.remove(0);

        let rows = payload
            .select()
            .unwrap()
            .map(|map| {
                let name = *map.get("name").unwrap();
                let name = name.into();

                GreetRow { name }
            })
            .collect::<Vec<_>>();

        assert_eq!(rows.len(), 1);

        println!("Hello {}!", rows[0].name); // Will always output "Hello World!"
    }
}

fn main() {
    #[cfg(feature = "sled-storage")]
    futures::executor::block_on(hello_world::run());
}
