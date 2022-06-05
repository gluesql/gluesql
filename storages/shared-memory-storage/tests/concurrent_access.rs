use {gluesql_core::prelude::Value, gluesql_shared_memory_storage::SharedMemoryStorage};

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).unwrap();
    };
}

#[tokio::test]
async fn concurrent_access() {
    use gluesql_core::prelude::{Glue, Payload};

    let storage = SharedMemoryStorage::new();

    let mut glue = Glue::new(storage.clone());
    exec!(glue "CREATE TABLE Thread (id INTEGER);");

    let thread_1 = tokio::spawn({
        // Arc::clone
        let storage = storage.clone();
        async {
            let mut glue = Glue::new(storage);
            exec!(glue "INSERT INTO Thread VALUES(1)");
        }
    });

    let thread_2 = tokio::spawn({
        // Arc::clone
        let storage = storage.clone();
        async {
            let mut glue = Glue::new(storage);
            exec!(glue "INSERT INTO Thread VALUES(2)");
        }
    });

    let _ = tokio::join!(thread_1, thread_2);

    let actual = glue.execute("SELECT * FROM Thread").unwrap();
    let expected = vec![Payload::Select {
        labels: vec!["id".to_owned()],
        rows: vec![vec![Value::I64(1)], vec![Value::I64(2)]],
    }];
    assert_eq!(actual, expected);
}
