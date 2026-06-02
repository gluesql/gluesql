use {
    gluesql_core::{
        prelude::{Error, Glue, Payload, Value},
        store::{Store, StoreMut},
    },
    gluesql_shared_memory_storage::SharedMemoryStorage,
    std::thread,
};

#[test]
fn concurrent_access() {
    let storage = SharedMemoryStorage::new();

    let mut glue = Glue::new(storage.clone());
    glue.execute("CREATE TABLE Thread (id INTEGER);").unwrap();

    let thread_1 = thread::spawn({
        let storage = storage.clone();
        move || {
            let mut glue = Glue::new(storage);
            glue.execute("INSERT INTO Thread VALUES(1)").unwrap();
        }
    });

    let thread_2 = thread::spawn({
        let storage = storage.clone();
        move || {
            let mut glue = Glue::new(storage);
            glue.execute("INSERT INTO Thread VALUES(2)").unwrap();
        }
    });

    thread_1.join().unwrap();
    thread_2.join().unwrap();

    let actual = glue.execute("SELECT * FROM Thread ORDER BY id").unwrap();
    let expected = vec![Payload::Select {
        labels: vec!["id".to_owned()],
        rows: vec![vec![Value::I64(1)], vec![Value::I64(2)]],
    }];
    assert_eq!(actual, expected);
}

#[test]
fn poisoned_lock_returns_storage_error() {
    let mut storage = SharedMemoryStorage::new();
    let database = storage.database.clone();

    let handle = thread::spawn(move || {
        let _guard = database.write().unwrap();
        panic!("poison shared memory lock");
    });

    assert!(handle.join().is_err());

    let expected = Err(Error::StorageMsg(
        "[Shared MemoryStorage] lock poisoned".to_owned(),
    ));
    assert_eq!(storage.fetch_all_schemas(), expected);

    let expected = Err(Error::StorageMsg(
        "[Shared MemoryStorage] lock poisoned".to_owned(),
    ));
    assert_eq!(storage.delete_schema("Foo"), expected);
}
