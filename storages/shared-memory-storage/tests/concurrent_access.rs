use {
    futures::executor::block_on,
    gluesql_core::prelude::{Glue, Payload, Value},
    gluesql_shared_memory_storage::SharedMemoryStorage,
};

#[tokio::test]
async fn concurrent_access() {
    let storage = SharedMemoryStorage::new();

    let mut glue = Glue::new(storage.clone());
    glue.execute("CREATE TABLE Thread (id INTEGER);")
        .await
        .unwrap();

    let thread_1 = tokio::spawn({
        // Arc::clone
        let storage = storage.clone();
        async {
            let mut glue = Glue::new(storage);
            block_on(glue.execute("INSERT INTO Thread VALUES(1)")).unwrap();
        }
    });

    let thread_2 = tokio::spawn({
        // Arc::clone
        let storage = storage.clone();
        async {
            let mut glue = Glue::new(storage);
            block_on(glue.execute("INSERT INTO Thread VALUES(2)")).unwrap();
        }
    });

    let _ = tokio::join!(thread_1, thread_2);

    let actual = glue.execute("SELECT * FROM Thread").await.unwrap();
    let expected = vec![Payload::Select {
        labels: vec!["id".to_owned()],
        rows: vec![vec![Value::I64(1)], vec![Value::I64(2)]],
    }];
    assert_eq!(actual, expected);
}
