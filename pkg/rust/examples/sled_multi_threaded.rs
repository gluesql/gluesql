#![allow(deprecated)]

// Legacy Sled-specific example retained to demonstrate its clone-based multi-threaded usage.
#[cfg(feature = "gluesql_sled_storage")]
mod sled_multi_threaded {
    use {
        gluesql::{
            gluesql_sled_storage::SledStorage,
            prelude::{Glue, Payload, Value},
        },
        std::{fs, sync::mpsc, thread},
    };

    pub fn run() {
        let sled_dir = "/tmp/gluesql/sled_multi_threaded";
        fs::remove_dir_all(sled_dir).unwrap_or(());
        let storage = SledStorage::new(sled_dir).expect("Something went wrong!");
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
        let (inserted_tx, inserted_rx) = mpsc::channel();
        let insert_storage = storage.clone();
        let insert_thread = thread::spawn(move || {
            let mut glue = Glue::new(insert_storage);
            let query = "INSERT INTO greet (name) VALUES ('Foo')";

            glue.execute(query).unwrap();
            inserted_tx.send(()).unwrap();
        });

        let select_storage = storage;
        let select_thread = thread::spawn(move || {
            inserted_rx.recv().unwrap();

            let mut glue = Glue::new(select_storage);
            let query = "SELECT * FROM greet;";

            let payloads = glue.execute(query).unwrap();
            println!("{payloads:?}");

            payloads
        });

        insert_thread
            .join()
            .expect("Something went wrong in the foo thread");

        let payloads = select_thread
            .join()
            .expect("Something went wrong in the world thread");
        assert_eq!(payloads.len(), 1);

        let Payload::Select { rows, .. } = &payloads[0] else {
            panic!("Unexpected result: {payloads:?}")
        };

        let first_row = &rows[0];
        let first_value = first_row.iter().next().unwrap();
        let to_greet = match first_value {
            Value::Str(to_greet) => to_greet,
            value => panic!("Unexpected type: {value:?}"),
        };

        // Outputs "Hello Foo!" after the reader observes the writer's committed row.
        println!("Hello {to_greet}!");
    }
}

fn main() {
    #[cfg(feature = "gluesql_sled_storage")]
    sled_multi_threaded::run();
}
