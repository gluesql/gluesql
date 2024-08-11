# Shared Memory Storage

SharedMemoryStorage is a storage option designed to provide more comfortable usage of MemoryStorage in concurrent environments. Although it doesn't operate in parallel, it makes accessing the same data from multiple threads simultaneously more convenient.

The basic structure of SharedMemoryStorage is straightforward. It wraps the MemoryStorage with a read-write lock (`RwLock`) and an atomic reference count (`Arc`):

```rust
#[derive(Clone, Debug)]
pub struct SharedMemoryStorage {
    pub database: Arc<RwLock<MemoryStorage>>,
}
```

This structure allows you to clone the storage instance and use it effortlessly across multiple threads. Regardless of how many times the storage is cloned, all storage instances will refer to the same data.

Here's an example of how to use SharedMemoryStorage in a concurrent environment:

```rust
use gluesql_core::prelude::{Glue, Payload, Value};

async fn concurrent_access() {
    let storage = SharedMemoryStorage::new();

    let mut glue = Glue::new(storage.clone());
    glue.execute("CREATE TABLE Thread (id INTEGER);").unwrap();

    let thread_1 = tokio::spawn({
        let storage = storage.clone();
        async {
            let mut glue = Glue::new(storage);
            glue.execute("INSERT INTO Thread VALUES(1)").unwrap();
        }
    });

    let thread_2 = tokio::spawn({
        let storage = storage.clone();
        async {
            let mut glue = Glue::new(storage);
            glue.execute("INSERT INTO Thread VALUES(2)").unwrap();
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
```

The `concurrent_access` function above illustrates how to concurrently insert data into the same table from different threads. After inserting data from two separate threads, we can confirm that the inserted data is correctly stored by executing a SELECT statement.

SharedMemoryStorage is primarily intended for convenience rather than performance when dealing with multiple threads. As you can see from the structure, placing a read-write lock (`RwLock`) on the entire database is not recommended for performance reasons when handling data concurrently from multiple threads. Therefore, it's best to use SharedMemoryStorage or MemoryStorage depending on the situation.

SharedMemoryStorage is only available in the Rust environment, and its implementation of the `Store` trait is identical to that of MemoryStorage.
