#[cfg(feature = "sled-storage")]
mod sled_multi_threaded {
    use {
        gluesql::{
            prelude::{Glue, Payload, Value},
            sled_storage::SledStorage,
        },
        std::thread,
    };

    pub fn run() {
        let storage = SledStorage::new("/tmp/gluesql/hello_world").expect("Something went wrong!");
        let mut glue = Glue::new(storage.clone());
        let queries = "
          CREATE TABLE IF NOT EXISTS greet (name TEXT);
          DELETE FROM greet;
        ";

        glue.execute(queries).unwrap();

        /*
            SledStorage supports cloning, using this we can create copies of the storage for new threads;
              all we need to do is wrap it in glue again.
        */
        let foo_storage = storage.clone();
        let foo_thread = thread::spawn(move || {
            let mut glue = Glue::new(foo_storage);
            let query = "INSERT INTO greet (name) VALUES (\"Foo\")";

            glue.execute(query).unwrap();
        });

        let world_storage = storage;
        let world_thread = thread::spawn(move || {
            let mut glue = Glue::new(world_storage);
            let query = "INSERT INTO greet (name) VALUES (\"World\")";

            glue.execute(query).unwrap();
        });

        world_thread
            .join()
            .expect("Something went wrong in the world thread");

        foo_thread
            .join()
            .expect("Something went wrong in the foo thread");

        let query = "SELECT name FROM greet";
        let payloads = glue.execute(query).unwrap();
        assert_eq!(payloads.len(), 1);

        let rows = match &payloads[0] {
            Payload::Select { rows, .. } => rows,
            _ => panic!("Unexpected result: {:?}", payloads),
        };

        let first_row = &rows[0];
        let first_value = &first_row[0];
        let to_greet = match first_value {
            Value::Str(to_greet) => to_greet,
            value => panic!("Unexpected type: {:?}", value),
        };

        // Will typically output "Hello Foo!" but will sometimes output "Hello World!"; depends on which thread finished first.
        println!("Hello {}!", to_greet);
    }
}

fn main() {
    #[cfg(feature = "sled-storage")]
    sled_multi_threaded::run();
}
