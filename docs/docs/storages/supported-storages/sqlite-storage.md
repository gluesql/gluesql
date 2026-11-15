# SQLite Storage

`SqliteStorage` stores GlueSQL tables as SQLite STRICT tables in a database file
or an in-memory database. It implements `Store`, `StoreMut`, `Transaction`, and
`NativeQuery` using a bundled SQLite library.

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

## Native filters

The SQLite planner can replace a single-table scan with a native query that
applies supported WHERE conditions. This also applies inside subqueries. SQLite
filters rows before GlueSQL decodes them; remaining filter conditions, projection
functions, joins, sorting, DISTINCT, OFFSET, and LIMIT use GlueSQL's executor.

Supported conditions include compatible integer, Text, Boolean, and Bytea
comparisons; Float32 and Float ordering comparisons; NULL checks; IN and BETWEEN;
and AND, OR, and NOT. Operands can be local columns, literals, or bound values.
Floating-point equality, mixed floating-point types, out-of-range integer
literals, functions, and arithmetic in WHERE remain in GlueSQL filters.
For a column declared `n INT8`, `n < 1000` remains common, while `n < 100` can be native.
Supported AND terms can be delegated independently. OR and NOT groups move only
when the whole group is supported. If no condition can be delegated, the original
filter and scan stay common.

Native conditions follow SQL three-valued NULL logic, matching the common
executor for supported AND/OR, IN/NOT IN, and BETWEEN/NOT BETWEEN expressions.

Native reads use the same connection and transaction as other storage methods.
They stream in rowid order, or logical primary-key order for integer, Boolean,
Text, and Bytea keys. Tables with other primary-key types keep the common scan.
Native execution errors are returned directly without retrying a common scan.

## Current scope

The common scan for tables without a primary key also streams in rowid order.
Both native and common scan iterators own their statement and cursor until
dropped, exhausted, or an error occurs. Row errors are returned when reached.

Primary key scans still collect and sort by GlueSQL's logical keys, including
keys stored as text. Decimal key lookup and whole-row updates can also collect a
table scan.

The backend currently supports tables created through GlueSQL. Compatibility
with databases created by other applications is future work. Explicit indexes,
custom stored functions, and the separate ALTER
TABLE feature set are not part of this first implementation. WebAssembly support
has not been validated.

Floating-point NaN is rejected because SQLite REAL cannot preserve it separately
from NULL. Tables must leave at least one of `_rowid_`, `rowid`, and `oid`
available for internal row identification. Nested transactions are not supported.
An explicit transaction whose commit fails because the database is busy remains
open for retry or rollback; failed automatic writes are rolled back.
