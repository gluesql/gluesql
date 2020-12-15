# Hello World

Assumes you have completed [installation.md](./installation.md).

```rust
use gluesql::{parse, Glue, Payload, SledStorage, Value};

fn main() {
    /*
        Initiate a connection
    */
    /*
        Open a Sled database, this will create one if one does not yet exist
    */
    let storage = SledStorage::new("/tmp/gluesql/hello_world").expect("Something went wrong!");
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

    let parsed_queries = parse(queries).expect("Parsing failed");
    for query in parsed_queries {
        glue.execute(&query).expect("Execution failed");
    }

    /*
        Select inserted row
    */
    let queries = "
      SELECT name FROM greet
    ";

    let parsed_query = &parse(queries).expect("Failed to parse query")[0];

    let result = glue.execute(&parsed_query).expect("Failed to execute");

    /*
        Query results are wrapped into a payload enum, on the basis of the query type
    */
    let rows = match result {
        Payload::Select { labels: _, rows } => rows,
        _ => panic!("Unexpected result: {:?}", result),
    };

    let first_row = &rows[0];
    let first_row_unwrapped = &first_row.0;
    let first_value = &first_row_unwrapped[0];

    /*
        Row values are wrapped into a value enum, on the basis of the result type
    */
    let to_greet = match first_value {
        Value::Str(to_greet) => to_greet,
        value => panic!("Unexpected type: {:?}", value),
    };

    println!("Hello {}!", to_greet);
}
```
