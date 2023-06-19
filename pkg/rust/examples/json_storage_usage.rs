#[cfg(feature = "json-storage")]
mod json_usage {
    use {
        gluesql::{
            json_storage::JsonStorage,
            prelude::{Glue, Payload, Value}
        }
    };

    // reference: https://gluesql.org/docs/0.14/storages/supported-storages/json-storage
    pub async fn run() {
        let path = "./data/";
        let json_storage = JsonStorage::new(path).unwrap();
        let mut glue = Glue::new(json_storage);

        glue.execute("
        CREATE TABLE User (
            id INT,
            name TEXT,
            location TEXT
        );
        ").await.expect("Execution failed");

        glue.execute("
        INSERT INTO User VALUES
            (1, 'Alice', 'New York'),
            (2, 'Bob', 'Rust'),
            (3, 'Eve');
        ").await.expect("Execution failed");

        glue.execute("
        CREATE TABLE LoginHistory (
            timestamp TEXT,
            userId INT,
            action TEXT
        );
        ").await.expect("Execution failed");

        glue.execute("
        INSERT INTO LoginHistory VALUES
            ('2023-05-01T14:36:22.000Z', 1, 'login'),
            ('2023-05-01T14:38:17.000Z', 2, 'logout'),
            ('2023-05-02T08:12:05.000Z', 2, 'logout'),
            ('2023-05-02T09:45:13.000Z', 3, 'login'),
            ('2023-05-03T16:21:44.000Z', 1, 'logout');
        ").await.expect("Execution failed");

        let result = glue.execute("
        SELECT *
        FROM User U
        JOIN LoginHistory L ON U.id = L.userId;
        ").await.expect("Execution failed");

        let rows = match &result[0] {
            Payload::Select { labels: _, rows } => rows,
            _ => panic!("Unexpected result: {:?}", result),
        };

        for row in rows {
            for value in row {
                match value {
                    Value::I64(i) => print!("{:?} ", i),
                    Value::Str(str) => print!("{:?} ", str),
                    Value::Null => print!("{:?} ", "NULL"),
                    _ => panic!("Unexpected value: {:?}", value),
                };
            }
            println!();
        }
    }
}

fn main() {
    #[cfg(feature = "json-storage")]
    futures::executor::block_on(json_usage::run());
}
