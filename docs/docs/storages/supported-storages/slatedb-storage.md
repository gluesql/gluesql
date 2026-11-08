# SlateDB Storage

`SlatedbStorage` stores GlueSQL schemas and rows in
[SlateDB](https://slatedb.io/), an embedded key-value engine backed by object
storage. Pass an `Arc<dyn ObjectStore>` and a database prefix to its constructor.
SlateDB manages the WAL, SST files, caching, and compaction.

The storage supports CRUD, primary-key lookups, table scans, and transactions.
It uses GlueSQL's default planner and `AlterTable` implementation. Secondary
indexes and custom functions are not supported; table metadata uses the default
empty result.

## Example

```rust
use {
    gluesql::{
        gluesql_slatedb_storage::{SlatedbStorage, object_store::memory::InMemory},
        prelude::Glue,
    },
    std::sync::Arc,
};

fn main() -> gluesql::core::error::Result<()> {
    let objects = Arc::new(InMemory::new());
    let storage = SlatedbStorage::new(objects.clone(), "my_database")?;
    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Items (id INTEGER PRIMARY KEY, name TEXT)")?;
    glue.execute("BEGIN")?;
    glue.execute("INSERT INTO Items VALUES (1, 'Alice')")?;
    glue.execute("COMMIT")?;
    glue.storage.close()?;

    // Reuse the same object store and prefix to reopen the database.
    let storage = SlatedbStorage::new(objects, "my_database")?;
    let mut glue = Glue::new(storage);
    println!("{:?}", glue.execute("SELECT * FROM Items WHERE id = 1")?);
    glue.storage.close()
}
```

`InMemory` needs no external service and is used by the storage's integration
tests. Its contents live only as long as the in-memory store is retained.

## S3 and Cloudflare R2

Use `object_store::aws::AmazonS3Builder` to configure S3 or R2, then pass the
built store to the same constructor. The `object_store` re-export uses the
version compatible with SlateDB.

```rust
use {
    gluesql::gluesql_slatedb_storage::{
        SlatedbStorage,
        object_store::aws::AmazonS3Builder,
    },
    std::sync::Arc,
};

// Configure AWS credentials using the object_store builder or environment.
let objects = AmazonS3Builder::from_env()
    .with_bucket("my-bucket")
    .with_region("us-east-1")
    .build()?;
let storage = SlatedbStorage::new(Arc::new(objects), "gluesql/my_database")?;
```

For R2, configure the account's S3-compatible endpoint, R2 access credentials,
and region `auto` on the builder. Bucket configuration belongs to the object
store; the constructor's prefix selects the database within that bucket.
Cloud endpoint and credential behavior requires validation in the deployment
environment; the local tests use InMemory.

## Transactions and naming

- One instance owns one writer and supports one active transaction. Use one
  writer per database prefix. Opening another writer at the same prefix can
  fence the existing writer.
- Transactions use SlateDB snapshot isolation. Reads inside a transaction see
  its own schema and row changes. Nested `BEGIN` is rejected.
- Successful `COMMIT` and successful autocommit writes wait for object-store
  durability. A failure while confirming durability does not establish that
  the commit was rolled back. The adapter does not retry SQL statements.
- An SQL error inside an explicit transaction follows GlueSQL's existing
  transaction behavior: the transaction stays active until `COMMIT` or
  `ROLLBACK`. No statement savepoints are added.
- Table names cannot contain `/`. Row keys and data values can contain `/`.
- Default ALTER operations collect and rewrite rows. Renaming a table also
  moves its rows to the new name's key prefix.
- `close()` discards any active transaction and reports shutdown errors.
  Dropping the storage also discards uncommitted changes and shuts down its DB.

## Calling from Tokio

The storage API is synchronous and owns a Tokio runtime for SlateDB's I/O and
background work. Keep creation, queries, and destruction inside a blocking
thread. For example, inside an async function:

```rust
tokio::task::spawn_blocking(|| -> gluesql::core::error::Result<()> {
    use {
        gluesql::{
            gluesql_slatedb_storage::{SlatedbStorage, object_store::memory::InMemory},
            prelude::Glue,
        },
        std::sync::Arc,
    };

    let storage = SlatedbStorage::new(Arc::new(InMemory::new()), "example")?;
    let mut glue = Glue::new(storage);
    glue.execute("CREATE TABLE Items (id INTEGER)")?;
    glue.storage.close()
})
.await??;
```

GlueSQL core remains synchronous. FileStorage/Parquet access to object stores
is separate from this row-oriented KV storage.
