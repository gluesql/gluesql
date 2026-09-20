# GlueSQL

[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![Chat](https://img.shields.io/discord/780298017940176946?logo=discord&logoColor=white)](https://discord.gg/C6TDEgzDzY)
[![Coverage Status](https://coveralls.io/repos/github/gluesql/gluesql/badge.svg?branch=main)](https://coveralls.io/github/gluesql/gluesql?branch=main)

## An Embeddable Multi-Model SQL Engine

> [**official documentation website**](https://gluesql.org/docs)

GlueSQL is a library for Multi-Model SQL databases written in Rust.

- Supports structured and unstructured data
- Supports a variety of storage options
- Supports custom storage backends through an extensible design
- Supports both SQL and Query Builder

## Installation

### Rust

```bash
cargo add gluesql
```

### JavaScript

```bash
npm install gluesql
```

### Browser (CDN)

```js
import { gluesql } from 'https://cdn.jsdelivr.net/npm/gluesql/gluesql.js';
```

For more information, check out the [gluesql-js repository](https://github.com/gluesql/gluesql-js).

## Supporting SQL and Query Builder

GlueSQL supports both SQL and Query Builder. Unlike ORMs that generate SQL strings, GlueSQL's Query Builder constructs execution-facing statement plans directly while still allowing explicit AST outputs where they are needed. This keeps access to GlueSQL-specific query features without routing every query through SQL text generation.

### [Rust Example](./pkg/rust/examples/hello_world.rs)

- example: [pkg/rust/examples/hello_world.rs](./pkg/rust/examples/hello_world.rs)

```rust
#[derive(gluesql::FromGlueRow)]
struct Row {
    id: i64,
    name: String,
}

let storage = MemoryStorage::default();
let mut glue = Glue::new(storage);

let rows = glue
    .execute("SELECT id, name FROM Foo;")
    .rows_as::<Row>()
    .unwrap();
```

### SQL Example

```sql
SELECT id, name FROM Foo WHERE name = 'Lemon' AND price > 100
```

### Query Builder Example

```rust
table("Foo")
    .select()
    // Filter by name using a SQL string
    .filter("name = 'Lemon'")
    // Filter by price using Query Builder methods
    .filter(col("price").gt(100))
    .project("id, name")
    .execute(&mut glue);
```

## Supporting Structured and Unstructured Data with Schema Flexibility

GlueSQL supports both structured and unstructured (schemaless) data. While SQL databases typically assume that schemas are defined and used, GlueSQL does not make this assumption. It supports completely unstructured data, similar to a NoSQL document database, as well as semi-structured types such as MAP and LIST. This makes GlueSQL suitable for a wide range of use cases, including those that require handling of unstructured data. Additionally, it is possible to join tables with schemas and schemaless tables together and execute queries.

### Schemaless SQL Example

```sql
CREATE TABLE Names (id INTEGER, name TEXT);
INSERT INTO Names VALUES (1, 'glue'), (2, 'sql');

CREATE TABLE Logs;
INSERT INTO Logs VALUES
    ('{ "id": 1, "value": 30 }'),
    ('{ "id": 2, "rate": 3.0, "list": [1, 2, 3] }'),
    ('{ "id": 3, "rate": 5.0, "value": 100 }');

SELECT * FROM Names JOIN Logs ON Names.id = Logs.id;
/*
| id | list    | name | rate | value |
|----|---------|------|------|-------|
| 1  |         | glue |      | 30    |
| 2  |[1, 2, 3]| sql  | 3    |       |
*/
```

## Supported Reference Storages

GlueSQL provides a variety of reference storages out of the box, including simple in-memory storage, key-value databases, and log file-based storage like JSON & JSONL. These reference storages are readily available for use and can be easily adapted to a variety of storage systems. Additionally, GlueSQL is constantly expanding its list of supported storages, making it a versatile tool for developers.

| Use case | Recommended storage |
| --- | --- |
| Temporary data, tests, and prototypes | [Memory](https://gluesql.org/docs/dev/storages/supported-storages/memory-storage/) |
| Shared in-memory data across threads | [Shared Memory](https://gluesql.org/docs/dev/storages/supported-storages/shared-memory-storage/) |
| Persistent embedded database | [Redb](https://gluesql.org/docs/dev/storages/supported-storages/redb-storage/) |
| Querying CSV, JSON, or Parquet files | [CSV](https://gluesql.org/docs/dev/storages/supported-storages/csv-storage/), [JSON](https://gluesql.org/docs/dev/storages/supported-storages/json-storage/), or [Parquet](https://gluesql.org/docs/dev/storages/supported-storages/parquet-storage/) |
| Lightweight filesystem persistence | [File](https://gluesql.org/docs/dev/storages/supported-storages/file-storage/) |
| Version-controlled data | [Git](https://gluesql.org/docs/dev/storages/supported-storages/git-storage/) |
| Existing MongoDB or Redis data | [Mongo](https://gluesql.org/docs/dev/storages/supported-storages/mongo-storage/) or [Redis](https://gluesql.org/docs/dev/storages/supported-storages/redis-storage/) |
| Queries across multiple storage backends | [Composite](https://gluesql.org/docs/dev/storages/supported-storages/composite-storage/) |

See the [Storage documentation](https://gluesql.org/docs/dev/storages/) for setup, examples, and limitations.

## Adapting GlueSQL to Your Environment: Creating Custom Storage

GlueSQL is designed to be adaptable to a wide variety of environments, including file systems, key-value databases, complex NoSQL databases, and remote APIs. To create a custom storage for GlueSQL, you only need to implement the Store and StoreMut traits provided by GlueSQL. For more information, check out [developing custom storages documentation](https://gluesql.org/docs/0.20.0/storages/developing-custom-storages/intro/).

## Contribution

GlueSQL is a database project that is simpler than you might think. You only need to know three common Rust project commands: `cargo fmt`, `cargo clippy`, and `cargo test`. Don't hesitate to make pull requests and change the code as you see fit. We have set up GitHub Actions to validate your changes, so you don't have to worry about making mistakes. The line coverage of GlueSQL's core code is almost 99%, which is the result of not only careful test writing, but also of making the test suite easy to understand and use for anyone, even those who are not familiar with Rust. If you're not sure where to start, we recommend exploring the test suite first. Take a look at the existing features and try to understand how they work. Even if you're not familiar with Rust, you should be able to navigate the test suite without any problems. If there's a feature you'd like to see but isn't there yet, implementing it yourself and contributing it to GlueSQL is a great way to get involved. You can also check out the issues on the GlueSQL GitHub repository for more ideas on how to contribute.

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](https://github.com/gluesql/gluesql/blob/main/LICENSE) file for details.
