# Redb Storage

RedbStorage allows GlueSQL to persist data using the [redb](https://github.com/cberner/redb) embedded key-value database. It provides ACID transactions, fast single-file access, and a stable API.

RedbStorage implements GlueSQL's `Store`, `StoreMut`, and `Transaction` traits.

## Example

```rust
use gluesql::{prelude::Glue, redb_storage::RedbStorage};

fn main() {
    let storage = RedbStorage::new("data/my_db.redb").unwrap();
    let mut glue = Glue::new(storage);

    let sql = "
        CREATE TABLE Foo (id INT, name TEXT);
        INSERT INTO Foo VALUES (1, 'Alice'), (2, 'Bob');
        SELECT * FROM Foo;
    ";

    let payloads = glue.execute(sql).unwrap();
    println!("{:#?}", payloads);
}
```

## Things to keep in mind

- Nested transactions are not supported.
- Only one RedbStorage instance should open the same database file at a time.

RedbStorage gives you an embedded, serverless database that integrates seamlessly with GlueSQL. Use `RedbStorage::new` to open or create a database file and execute SQL through `Glue`.

## File format migration

GlueSQL continues to use the redb 2.6 crate, but new databases use redb's internal file format v3. GlueSQL's redb storage format version is also v3; this is separate metadata used to require the file upgrade, not the redb crate version.

Existing redb storage format v1 or v2 files must be upgraded once before opening them:

```shell
gluesql --storage redb --path data.redb --upgrade
```

Library users can call `gluesql_redb_storage::migrate_to_latest(path)` instead. The migration preserves the existing row serialization while upgrading the redb file from v2 to v3. Back up the database first because older GlueSQL releases reject storage format v3 after the upgrade.
