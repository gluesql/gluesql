# SQLite Storage

`SqliteStorage` stores GlueSQL tables as SQLite STRICT tables in a database file
or an in-memory database. It implements `Store`, `StoreMut`, and `Transaction`
using a bundled SQLite library.

## Example

Enable the `gluesql-sqlite-storage` feature when using GlueSQL without its default
features:

```toml
[dependencies]
gluesql = { version = "0.20.0", default-features = false, features = ["gluesql-sqlite-storage"] }
```

```rust
use gluesql::prelude::{Glue, SqliteStorage};

fn main() {
    let storage = SqliteStorage::new("data.sqlite").unwrap();
    let mut glue = Glue::new(storage);
    let result = glue.execute(
        "CREATE TABLE IF NOT EXISTS item (id INT PRIMARY KEY, name TEXT);
         BEGIN;
         INSERT INTO item VALUES (1, 'apple');
         COMMIT;
         SELECT * FROM item;"
    ).unwrap();
    println!("{result:#?}");
}
```

Use `SqliteStorage::in_memory()` for a database owned by one storage instance.
Reopening a file created by this backend restores its GlueSQL schema and values.

## Stored values

| GlueSQL values | SQLite representation |
| --- | --- |
| Signed integers through 64 bits, unsigned integers through 32 bits, Boolean | INTEGER; Boolean uses 0 and 1 |
| Float32, Float | REAL |
| Text, Bytea, Null | TEXT, BLOB, NULL |
| Uint64, Int128, Uint128, Decimal | Decimal text; Decimal retains its scale |
| Date, Time, Timestamp, UUID, Inet | Text in the corresponding value's string format |
| Map, List, Interval, Point | Typed RON text |

Two internal tables, `__gluesql_table_meta` and `__gluesql_column_meta`, preserve
logical types, comments, schemaless identity, and default expressions that are not
written into the SQLite table definition. These names are reserved. Schemaless
tables store one Map value per row in a `_doc` TEXT column.

## Current scope

Queries use GlueSQL's planner and executor. Predicates, joins, and other query
operations are not yet delegated to SQLite. Tables without a primary key stream
rows in SQLite rowid order. Their scan iterator owns the prepared statement and
cursor until it is dropped, exhausted, or encounters an error. Row read errors
are returned as iterator items when those rows are reached.

Primary key scans still collect and sort by GlueSQL's logical keys, including
keys stored as text. Key lookup and whole-row updates can also collect a table
scan.

The backend currently supports tables created through GlueSQL. Compatibility
with databases created by other applications and native query delegation are
future work. Explicit indexes, custom stored functions, and the separate ALTER
TABLE feature set are not part of this first implementation. WebAssembly support
has not been validated.

Floating-point NaN is rejected because SQLite REAL cannot preserve it separately
from NULL. Tables must leave at least one of `_rowid_`, `rowid`, and `oid`
available for internal row identification. Nested transactions are not supported.
An explicit transaction whose commit fails because the database is busy remains
open for retry or rollback; failed automatic writes are rolled back.
