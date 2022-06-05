use gluesql_shared_memory_storage::SharedMemoryStorage;

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

    let payloads = glue.execute("SELECT * FROM Thread").unwrap();
    assert_eq!(payloads.len(), 1);

    let payload = &payloads[0];
    let rows = match &payload {
        Payload::Select { rows, .. } => rows.iter().flatten().collect::<Vec<_>>(),
        _ => unreachable!(),
    };
    assert_eq!(rows.len(), 2);
}
