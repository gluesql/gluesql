use {
    gluesql::{parse, Glue, Payload, SledStorage, Value},
    std::thread,
};

fn main() {
    let storage = SledStorage::new("/tmp/gluesql/hello_world").expect("Something went wrong!");
    let mut glue = Glue::new(storage.clone());
    let queries = "
      CREATE TABLE IF NOT EXISTS greet (name TEXT);
      DELETE FROM greet;
    ";

    let parsed_queries = parse(queries).expect("Parsing failed");
    for query in parsed_queries {
        glue.execute(&query).expect("Execution failed");
    }
    /*
        SledStorage supports cloning, using this we can create copies of the storage for new threads;
          all we need to do is wrap it in glue again.
    */
    let foo_storage = storage.clone();
    let foo_thread = thread::spawn(move || {
        let mut glue = Glue::new(foo_storage);
        let queries = "INSERT INTO greet (name) VALUES (\"Foo\")";
        let parsed_queries = parse(queries).expect("Parsing failed");
        for query in parsed_queries {
            glue.execute(&query).expect("Execution failed");
        }
    });

    let world_storage = storage.clone();
    let world_thread = thread::spawn(move || {
        let mut glue = Glue::new(world_storage);
        let queries = "INSERT INTO greet (name) VALUES (\"World\")";
        let parsed_queries = parse(queries).expect("Parsing failed");
        for query in parsed_queries {
            glue.execute(&query).expect("Execution failed");
        }
    });

    world_thread
        .join()
        .expect("Something went wrong in the world thread");

    foo_thread
        .join()
        .expect("Something went wrong in the foo thread");

    let queries = "
      SELECT name FROM greet
    ";

    let parsed_query = &parse(queries).expect("Failed to parse query")[0];

    let result = glue.execute(&parsed_query).expect("Failed to execute");

    let rows = match result {
        Payload::Select { labels: _, rows } => rows,
        _ => panic!("Unexpected result: {:?}", result),
    };

    let first_row = &rows[0];
    let first_row_unwrapped = &first_row.0;
    let first_value = &first_row_unwrapped[0];
    let to_greet = match first_value {
        Value::Str(to_greet) => to_greet,
        value => panic!("Unexpected type: {:?}", value),
    };

    println!("Hello {}!", to_greet); // Will typically output "Hello Foo!" but will sometimes output "Hello World!"; depends on which thread finished first.
}