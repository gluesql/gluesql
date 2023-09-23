#[cfg(feature = "sled-storage")]
mod hello_world {
    use {
        gluesql::{
            prelude::{Glue, Payload},
            sled_storage::SledStorage,
        },
        std::fs,
    };

    struct GreetTable {
        rows: Vec<GreetRow>,
    }
    struct GreetRow {
        name: String,
    }

    impl TryFrom<Payload> for GreetTable {
        type Error = &'static str;

        fn try_from(payload: Payload) -> Result<Self, Self::Error> {
            match payload {
                Payload::Select { labels: _, rows } => {
                    let rows = rows
                        .into_iter()
                        .map(|mut row| {
                            let name = row.remove(0);
                            GreetRow {
                                name: String::from(name),
                            }
                        })
                        .collect::<Vec<_>>();

                    Ok(Self { rows })
                }
                Payload::SelectMap(rows) => {
                    let rows = rows
                        .into_iter()
                        .map(|row| {
                            let name = row.get("name").unwrap();
                            GreetRow {
                                name: String::from(name),
                            }
                        })
                        .collect::<Vec<_>>();

                    Ok(Self { rows })
                }
                _ => Err("unexpected payload, expected a select query result"),
            }
        }
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

        let table: GreetTable = result.remove(0).try_into().unwrap();
        assert_eq!(table.rows.len(), 1);

        println!("Hello {}!", table.rows[0].name); // Will always output "Hello World!"
    }
}

fn main() {
    #[cfg(feature = "sled-storage")]
    futures::executor::block_on(hello_world::run());
}
