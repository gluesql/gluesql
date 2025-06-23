# Redb Storage

RedbStorage allows GlueSQL to persist data using the [redb](https://github.com/cberner/redb) embedded key-value database. It provides ACID transactions, fast single-file access, and a stable API.

RedbStorage implements GlueSQL's `Store`, `StoreMut`, and `Transaction` traits.
## Example

```rust
use gluesql::{prelude::Glue, redb_storage::RedbStorage};

#[tokio::main]
async fn main() {
    let storage = RedbStorage::new("data/my_db.redb").unwrap();
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

## Things to keep in mind

- Nested transactions are not supported.
- Only one RedbStorage instance should open the same database file at a time.

RedbStorage gives you an embedded, serverless database that integrates seamlessly with GlueSQL. Use `RedbStorage::new` to open or create a database file and execute SQL through `Glue`.
