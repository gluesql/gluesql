# GlueSQL

[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![Chat](https://img.shields.io/discord/780298017940176946?logo=discord&logoColor=white)](https://discord.gg/C6TDEgzDzY)
[![Coverage Status](https://coveralls.io/repos/github/gluesql/gluesql/badge.svg?branch=main)](https://coveralls.io/github/gluesql/gluesql?branch=main)

## An Embeddable Multi-Model SQL Engine

GlueSQL is an embeddable, multi-model SQL database engine written in Rust.
It combines SQL with the flexibility to work across different data models and storage environments.
It is also available for JavaScript applications in the browser and Node.js.

## Why GlueSQL?

GlueSQL is quite sticky: it brings SQL to your application's storage. Use a provided storage backend or implement a custom adapter to query existing data without moving it into a separate database. GlueSQL handles SQL parsing, planning, and execution.

- **Choose a storage backend:** use in-memory storage, embedded databases, local files, or external databases.
- **Work with flexible data:** query schema-defined and schemaless tables together, including MAP and LIST values.
- **Choose a query interface:** write SQL or compose queries with the Rust Query Builder, both backed by the same engine.

[Explore the documentation →](https://gluesql.org/docs)

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

For more JS library information, check out the [gluesql-js repository](https://github.com/gluesql/gluesql-js).

## Supported Reference Storages

GlueSQL provides reference storage implementations for in-memory data, embedded databases, local files, and external databases. Use the table below to choose a storage for your use case.

| Use case | Recommended storage |
| --- | --- |
| Persistent embedded database | [Redb](https://gluesql.org/docs/0.20.0/storages/supported-storages/redb-storage/) |
| Existing MongoDB or Redis data | [Mongo](https://gluesql.org/docs/0.20.0/storages/supported-storages/mongo-storage/) or [Redis](https://gluesql.org/docs/0.20.0/storages/supported-storages/redis-storage/) |
| Temporary data, tests, and prototypes, data across threads | [Memory](https://gluesql.org/docs/0.20.0/storages/supported-storages/memory-storage/), [Shared Memory](https://gluesql.org/docs/0.20.0/storages/supported-storages/shared-memory-storage/) |
| Querying CSV, JSON, or Parquet files | [CSV](https://gluesql.org/docs/0.20.0/storages/supported-storages/csv-storage/), [JSON](https://gluesql.org/docs/0.20.0/storages/supported-storages/json-storage/), or [Parquet](https://gluesql.org/docs/0.20.0/storages/supported-storages/parquet-storage/) |
| Lightweight filesystem persistence | [File](https://gluesql.org/docs/0.20.0/storages/supported-storages/file-storage/) |
| Version-controlled data | [Git](https://gluesql.org/docs/0.20.0/storages/supported-storages/git-storage/) |
| Queries across multiple storage backends | [Composite](https://gluesql.org/docs/0.20.0/storages/supported-storages/composite-storage/) |
| Browser / JS applications | [JavaScript: memory, local Storage, or OPFS](https://github.com/gluesql/gluesql-js#pick-the-storage-that-matches-your-data) |

See the [Storage documentation](https://gluesql.org/docs/0.20.0/storages/) for setup, examples, and limitations.

## SQL and Query Builder

GlueSQL supports both SQL and a Query Builder. Use SQL for familiar or dynamic queries, and use the Query Builder when composing queries in Rust or controlling execution more precisely. Both interfaces run through the same GlueSQL engine and storage backend. For more information, check out the [Query Builder documentation](https://gluesql.org/docs/0.20.0/query-builder/intro/) and [SQL documentation](https://gluesql.org/docs/0.20.0/sql-syntax/intro/).

### SQL

```sql
SELECT id, name FROM Foo WHERE name = 'Lemon' AND price > 100
```

### Query Builder

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

Unlike ORM query builders designed to support multiple database engines by generating SQL, GlueSQL's Query Builder builds executable statement plans directly. It accepts both builder methods and SQL expressions, supports the full GlueSQL feature set, and can express execution details that SQL can only suggest through query hints.

## Supporting Structured and Unstructured Data with Schema Flexibility

GlueSQL supports both structured and unstructured (schemaless) data. Unlike traditional SQL databases, it does not require every table to have a predefined schema. Schemaless tables can store varying fields, while MAP and LIST support semi-structured values. Schema-defined and schemaless tables can also be joined in the same query.

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

## Adapting GlueSQL to Your Environment: Creating Custom Storage

GlueSQL is designed to be adaptable to a wide variety of environments, including file systems, key-value databases, complex NoSQL databases, and remote APIs. To create a custom storage for GlueSQL, you only need to implement the Store and StoreMut traits provided by GlueSQL. For more information, see the [custom storage development documentation](https://gluesql.org/docs/0.20.0/storages/developing-custom-storages/intro/).

## Contributing

GlueSQL is simpler to contribute to than it may look. Its test suite and continuous integration provide guardrails that catch regressions before changes are merged, so don't hesitate to open an issue or pull request.

If you're not sure where to start, explore the [test suite](test-suite). The [SQL fixtures](test-suite/fixtures) are a good starting point for getting a sense of GlueSQL's overall capabilities. Try contributing a feature you'd like to use, or browse the [GitHub issues](https://github.com/gluesql/gluesql/issues) for more ideas.

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](https://github.com/gluesql/gluesql/blob/main/LICENSE) file for details.
