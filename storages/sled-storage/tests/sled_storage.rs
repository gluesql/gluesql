use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_sled_storage::SledStorage,
    test_suite::*,
};

struct SledTester {
    glue: Glue<SledStorage>,
}

#[async_trait(?Send)]
impl Tester<SledStorage> for SledTester {
    async fn new(namespace: &str) -> Self {
        let path = format!("data/{}", namespace);

        match std::fs::remove_dir_all(&path) {
            Ok(()) => (),
            Err(e) => {
                println!("fs::remove_file {:?}", e);
            }
        }

        let config = sled::Config::default()
            .path(path)
            .temporary(true)
            .mode(sled::Mode::HighThroughput);

        let storage = SledStorage::try_from(config).expect("SledStorage::new");
        let glue = Glue::new(storage);

        SledTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<SledStorage> {
        &mut self.glue
    }
}

#[tokio::test]
async fn sled_basic() {
    use gluesql_core::prelude::*;

    let mut tester = SledTester::new("delete").await;
    let glue = tester.get_glue();

    macro_rules! execute {
        ($sql:expr) => {{
            let mut payloads = glue.execute($sql).await.unwrap();
            payloads.remove(0)
        }};
    }

    macro_rules! select {
        ($label: literal, $rows:expr) => {
            Payload::Select {
                labels: vec![$label.to_owned()],
                rows: $rows,
            }
        };

        ($label: literal) => {
            Payload::Select {
                labels: vec![$label.to_owned()],
                rows: vec![],
            }
        };
    }

    execute!("CREATE TABLE Foo(id INTEGER PRIMARY KEY);");
    execute!("INSERT INTO Foo VALUES (1), (2);");

    assert_eq!(
        execute!("SELECT * FROM Foo;"),
        select!("id", vec![vec![Value::I64(1)], vec![Value::I64(2)]])
    );

    execute!("DELETE FROM Foo where id = 1;");
    assert_eq!(
        execute!("SELECT * FROM Foo;"),
        select!("id", vec![vec![Value::I64(2)]])
    );

    execute!("INSERT INTO Foo VALUES(1);");
    assert_eq!(
        execute!("SELECT * FROM Foo;"),
        select!("id", vec![vec![Value::I64(1)], vec![Value::I64(2)]])
    );
}

generate_store_tests!(tokio::test, SledTester);
generate_index_tests!(tokio::test, SledTester);
generate_transaction_tests!(tokio::test, SledTester);
generate_alter_table_tests!(tokio::test, SledTester);
generate_alter_table_index_tests!(tokio::test, SledTester);
generate_transaction_alter_table_tests!(tokio::test, SledTester);
generate_transaction_index_tests!(tokio::test, SledTester);
generate_metadata_index_tests!(tokio::test, SledTester);
