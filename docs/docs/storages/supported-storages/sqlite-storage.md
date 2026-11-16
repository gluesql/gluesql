# SQLite Storage

SQLite has its own SQL engine. With `SqliteStorage`, you choose SQLite as the
backend for GlueSQL. You still write queries using GlueSQL's SQL syntax, types,
and functions. SQLite stores the data, and you use the same `Glue` API as with
other backends.

The goal is to use SQLite's query engine too, so GlueSQL can read and process
less data. A planner in GlueSQL decides which parts of a query SQLite should run.
GlueSQL handles the rest. We are extending this planner step by step to run more
operations inside SQLite.

This backend currently supports tables created through GlueSQL. Tables created
by other SQLite applications are not yet supported.

## Getting started

Enable the SQLite storage feature in your Rust project. SQLite is bundled with
the storage crate.

```toml
[dependencies.gluesql]
version = "0.20.0"
default-features = false
features = ["gluesql-sqlite-storage"]
```

For a new database, open a file and create a table through `Glue`:

```rust
use gluesql::prelude::{Glue, SqliteStorage};

fn main() {
    let storage = SqliteStorage::new("data.sqlite").unwrap();
    let mut glue = Glue::new(storage);

    let sql = r#"
        CREATE TABLE Item (id INT PRIMARY KEY, name TEXT, qty INT, tags LIST);
        INSERT INTO Item VALUES
            (1, 'Apple', 20, '["fruit"]'),
            (2, 'Pear', 30, '[]'),
            (3, 'Apple', 5, '["fruit"]');
        SELECT name, qty, tags FROM Item ORDER BY id;
    "#;

    let payloads = glue.execute(sql).unwrap();
    println!("{payloads:#?}");
}
```

The `tags` column uses GlueSQL's [LIST](../../sql-syntax/data-types/list.md) type
to store each item's tags.

The data remains in `data.sqlite` after the program exits. To query it again,
open the same path with `SqliteStorage::new` and run your queries without
recreating the table.

Use `SqliteStorage::in_memory()` for a temporary database. Its data lasts until
the storage instance is dropped.

## How queries use SQLite

When you call `glue.execute`, GlueSQL plans the query before running it. Its
SQLite-specific planner chooses which parts to send to SQLite and translates
them into SQLite SQL. SQLite's own planner decides how to run that SQL, and its
engine executes it. GlueSQL uses the returned rows to finish the query.

The first optimization handles some WHERE conditions on a single table in SELECT
queries. SQLite filters the rows before returning them to GlueSQL. GlueSQL
evaluates the other conditions and handles joins, aggregates, sorting, and writes.

For example, using the `Item` table created above, this query finds items with a
quantity greater than 10 and at least one tag:

```sql
SELECT name, qty
FROM Item
WHERE qty > 10 AND NOT IS_EMPTY(tags);
```

[`IS_EMPTY`](../../sql-syntax/functions/list-map/is-empty.md) checks whether a
GlueSQL list or map is empty. SQLite has no built-in `IS_EMPTY` function.

The planner sends `qty > 10` to SQLite, which returns the two matching rows:
`Apple` with quantity 20 and `Pear` with quantity 30. GlueSQL then checks their
tag lists. `Apple` has the `fruit` tag and `Pear` has none, so the query returns
only `Apple` with quantity 20.

Submit the whole query to `glue.execute` just as in the first example. The planner
chooses where to run each part; you do not need to split the query yourself.

## Transactions

Use [`BEGIN`, `COMMIT`, and `ROLLBACK`](../../sql-syntax/statements/transaction.md)
to group writes into a transaction. Queries in that transaction see your changes,
including when SQLite handles part of the filtering. Nested transactions are not
supported.

If an explicit `COMMIT` fails because the database is busy, the transaction stays
open. You can retry `COMMIT` or discard the changes with `ROLLBACK`. Writes made
outside an explicit transaction are committed automatically and rolled back if
they fail.

## Things to keep in mind

- Explicit indexes, custom stored functions, and `ALTER TABLE` are not yet
  supported by this backend.
- Floating-point NaN values cannot be stored. SQLite cannot preserve them
  separately from NULL.
- Leave at least one of `_rowid_`, `rowid`, and `oid` unused as a column name in
  each table. The backend needs one of these names to identify rows internally.

WebAssembly support has not been validated.

## Inspecting the database file

You can open the file with SQLite tools to inspect the stored data. GlueSQL
creates SQLite STRICT tables and keeps additional schema information in
`__gluesql_table_meta` and `__gluesql_column_meta`. These table names are reserved;
keep the metadata tables intact so GlueSQL can restore the schema when reopening
the file.

Some values look different in SQLite tools. For example, DECIMAL values are
stored as text, and MAP and LIST values use RON (Rusty Object Notation) text,
which SQLite's JSON functions cannot read. Querying through GlueSQL restores
these values to their GlueSQL types automatically.
