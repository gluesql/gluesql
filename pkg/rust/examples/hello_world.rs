#[cfg(feature = "sled-storage")]
mod hello_world {
    use {
        gluesql::{
            prelude::{Glue, Payload, Value},
            sled_storage::SledStorage,
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
            CREATE TABLE student (uuid_field UUID, name TEXT, dept_name TEXT, luggage LIST);
            INSERT INTO student VALUES (generate_uuid(), \'이의제\', \'CSE\', \'[\"MacBook\", 10000]\');
            INSERT INTO student VALUES (generate_uuid(), \'채승운\', \'CSE\', \'[\"MacBook\", \"iPad\"]\');
        ";

        glue.execute(queries).expect("Execution failed");

        /*
            Select inserted row
        */
        let queries = "
            SELECT * FROM student;
        ";

        let result = glue.execute(queries).expect("Failed to execute");

        /*
            Query results are wrapped into a payload enum, on the basis of the query type
        */
        assert_eq!(result.len(), 1);
        // let _my_labels = vec!["*"];
        let rows = match &result[0] {
            Payload::Select { rows, .. } => rows,
            _ => panic!("Unexpected result: {:?}", result),
        };
        
        for row in rows.iter() {
            /*
                Row values are wrapped into a value enum, on the basis of the result type
            */
            for val in row.iter() {
                match val {
                    Value::Str(val) => print!("{} ", val),
                    Value::Uuid(val) => print!("{} ", val),
                    Value::List(val) => print!("{:?} ", val),
                    value => panic!("Unexpected type: {:?}", value),
                };
            }

            println!();
        }
    }
}

fn main() {
    /* cargo run -- --path /tmp/gluesql/hello_world --storage=sled */
    #[cfg(feature = "sled-storage")]
    hello_world::run();
}   
