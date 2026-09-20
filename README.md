# GlueSQL

[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![Chat](https://img.shields.io/discord/780298017940176946?logo=discord&logoColor=white)](https://discord.gg/C6TDEgzDzY)
[![Coverage Status](https://coveralls.io/repos/github/gluesql/gluesql/badge.svg?branch=main)](https://coveralls.io/github/gluesql/gluesql?branch=main)

## An Embeddable Multi-Model SQL Engine

> [**Official Documentation Website**](https://gluesql.org/docs)

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

## SQL and Query Builder

GlueSQL supports both SQL and Query Builder. Use SQL for familiar or dynamic queries, and use Query Builder when composing queries in Rust or controlling execution more precisely. Both interfaces run through the same GlueSQL engine and storage backend. For more information, check out the [query builder documentation](https://gluesql.org/docs/0.20.0/query-builder/intro/) and [SQL documentation](https://gluesql.org/docs/0.20.0/sql-syntax/intro/).

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

Unlike ORM query builders that generate SQL for multiple database engines, GlueSQL's Query Builder builds executable statement plans directly for GlueSQL. It accepts both builder methods and SQL expressions, supports the full GlueSQL feature set, and can express execution details that SQL can only suggest through query hints.

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
| Temporary data, tests, and prototypes | [Memory](https://gluesql.org/docs/0.20.0/storages/supported-storages/memory-storage/) |
| Shared in-memory data across threads | [Shared Memory](https://gluesql.org/docs/0.20.0/storages/supported-storages/shared-memory-storage/) |
| Persistent embedded database | [Redb](https://gluesql.org/docs/0.20.0/storages/supported-storages/redb-storage/) |
| Querying CSV, JSON, or Parquet files | [CSV](https://gluesql.org/docs/0.20.0/storages/supported-storages/csv-storage/), [JSON](https://gluesql.org/docs/0.20.0/storages/supported-storages/json-storage/), or [Parquet](https://gluesql.org/docs/0.20.0/storages/supported-storages/parquet-storage/) |
| Lightweight filesystem persistence | [File](https://gluesql.org/docs/0.20.0/storages/supported-storages/file-storage/) |
| Version-controlled data | [Git](https://gluesql.org/docs/0.20.0/storages/supported-storages/git-storage/) |
| Existing MongoDB or Redis data | [Mongo](https://gluesql.org/docs/0.20.0/storages/supported-storages/mongo-storage/) or [Redis](https://gluesql.org/docs/0.20.0/storages/supported-storages/redis-storage/) |
| Queries across multiple storage backends | [Composite](https://gluesql.org/docs/0.20.0/storages/supported-storages/composite-storage/) |

See the [Storage documentation](https://gluesql.org/docs/0.20.0/storages/) for setup, examples, and limitations.

## Adapting GlueSQL to Your Environment: Creating Custom Storage

GlueSQL is designed to be adaptable to a wide variety of environments, including file systems, key-value databases, complex NoSQL databases, and remote APIs. To create a custom storage for GlueSQL, you only need to implement the Store and StoreMut traits provided by GlueSQL. For more information, check out [developing custom storages documentation](https://gluesql.org/docs/0.20.0/storages/developing-custom-storages/intro/).

## Contributing

GlueSQL is simpler to contribute to than it may look. Its test suite and continuous integration provide guardrails that catch regressions before changes are merged, so don't hesitate to open an issue or pull request. See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow, testing commands, and pull request guidelines.

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](https://github.com/gluesql/gluesql/blob/main/LICENSE) file for details.
