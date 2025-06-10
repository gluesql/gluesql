# Redis Storage

Redis Storage lets GlueSQL use a running Redis server for persisting tables and rows. It provides the standard GlueSQL API so you can send SQL statements while the data is stored inside Redis.

## Prerequisites

You need a running Redis instance. The easiest way is with Docker:

```bash
docker run --name redis-glue -p 6379:6379 -d redis
```

Alternatively install Redis locally from the [official downloads](https://redis.io/download).

## Enabling the Storage

Add `gluesql-redis-storage` to your `Cargo.toml`:

```toml
[dependencies]
gluesql-redis-storage = "*"
```

Then create the storage by specifying a namespace and connection details.

## Example

```rust
use gluesql::{prelude::Glue, redis_storage::RedisStorage};

#[tokio::main]
async fn main() {
    // connect to Redis on localhost:6379 using namespace "my_db"
    let storage = RedisStorage::new("my_db", "localhost", 6379);
    let mut glue = Glue::new(storage);

    let sql = "
        CREATE TABLE Foo (id INT, name TEXT);
        INSERT INTO Foo VALUES (1, 'Alice'), (2, 'Bob');
        SELECT * FROM Foo;
    ";

    let payloads = glue.execute(sql).await.unwrap();
    println!("{:#?}", payloads);
}
```

Running this program prints the results of the final SELECT and leaves the data inside your Redis instance.
