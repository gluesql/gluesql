# SQLite Storage

SQLite Storage lets GlueSQL operate directly on top of an existing SQLite database file (or an in-memory SQLite instance). You can point GlueSQL at data that was created outside of GlueSQL, then immediately combine it with any of the other storages GlueSQL offers.

## Example

```rust
use gluesql_core::{error::Result, prelude::Glue};
use gluesql_sqlite_storage::SqliteStorage;

#[tokio::main]
async fn main() -> Result<()> {
    // Open or create an SQLite database file.
    let storage = SqliteStorage::new("app.db").await?;
    let mut glue = Glue::new(storage);

    glue.execute("
        CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
        SELECT * FROM users;
    ").await?;

    Ok(())
}
```

`SqliteStorage::memory()` is also available when you want an in-memory database while keeping SQLite compatibility.

## Why use it?

- **Reuse existing data**: keep your current SQLite file but gain GlueSQL conveniences such as schemaless tables, AST Builder support, and SQL/NoSQL joins.
- **Cross-storage workflows**: join SQLite tables with data coming from memory, JSON, MongoDB, and other GlueSQL storages. It is handy for analytics or gradual migrations.
- **Pipelines and migrations**: move rows between SQLite and other GlueSQL-backed stores step by step, all from within GlueSQL's execution layer.
- **Transactions ready**: the storage implements GlueSQL's `Transaction` trait, letting you keep transactional semantics across your GlueSQL queries.

The storage automatically persists GlueSQL's additional schema metadata inside SQLite so the same file can be opened alternately by native SQLite tooling and GlueSQL.

## How it works

- **Type mapping**: GlueSQL values are mapped onto SQLite's dynamic typing rules. Integer-like GlueSQL keys are stored as SQLite INTEGER, real numbers as REAL, while larger integers, decimals, UUIDs, and other string-based types are serialized as TEXT. Binary data uses BLOB; Map/List/Interval/Point and other complex types are serialized to JSON or string representations.
- **Schema metadata**: When GlueSQL creates or discovers a table, it writes the schema definition into SQLite's metadata (using a `gluesql_schema` table and annotated CREATE TABLE statements). Native SQLite tools see a regular table, and GlueSQL can reconstruct the richer GlueSQL schema when it opens the file again.
- **Schemaless tables**: If a table is created without column definitions, GlueSQL stores rows in a single `_gluesql_payload` TEXT column as JSON. GlueSQL can read and manipulate those schemaless rows, while native SQLite tools still see a valid table.
- **Row identification**: Primary keys use the designated column; tables without a primary key fall back to SQLite's `rowid`. GlueSQL converts between its `Key` variants and SQLite parameters transparently.

Because of this design, GlueSQL does not require any external service or migration step: you point it at a `.sqlite` file, and it gains the same multi-storage capabilities as any other GlueSQL backend.
