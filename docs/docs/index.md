---
sidebar_position: 1
---

# Introduction

[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![npm](https://img.shields.io/npm/v/gluesql?color=red)](https://www.npmjs.com/package/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![Chat](https://img.shields.io/discord/780298017940176946?logo=discord&logoColor=white)](https://discord.gg/C6TDEgzDzY)
[![Coverage Status](https://coveralls.io/repos/github/gluesql/gluesql/badge.svg?branch=main)](https://coveralls.io/github/gluesql/gluesql?branch=main)

## Multi-Model Database Engine as a Library
GlueSQL is a Rust library for SQL databases that includes a parser ([sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)), an execution layer, and a variety of storage options, both persistent and non-persistent, all in one package. It is a versatile tool for developers, supporting both SQL and its own query builder (AST Builder). GlueSQL can handle structured and unstructured data, making it suitable for a wide range of use cases. It is portable and can be used with various storage types, including log files and read-write capable storage. GlueSQL is designed to be extensible and supports custom planners, making it a powerful tool for developers who need SQL support for their databases or services. GlueSQL is also flexible, as it can be used in Rust and JavaScript environments, and its language support is constantly expanding to include more programming languages.

"We offer a service where the GlueSQL team can implement and maintain your custom storage, especially beneficial for NoSQL databases with their own query planner and execution layer. We welcome any services wishing to support SQL and GlueSQL query interfaces. For more details, please refer to [**here**](#gluesql-custom-storage-let-us-handle-it-for-you)."

If you're interested in learning more about GlueSQL, we recommend the following blog articles for a deeper dive into its capabilities and benefits:

1. [Breaking the Boundary between SQL and NoSQL Database](https://gluesql.org/blog/breaking-the-boundary-between-sql-and-nosql)
2. [Revolutionizing Databases by Unifying Query Interfaces](https://gluesql.org/blog/revolutionizing-databases-by-unifying-query-interfaces)
3. [Test-Driven Documentation - Automating User Manual Creation](https://gluesql.org/blog/test-driven-documentation)

## Supporting SQL and AST Builder
GlueSQL supports both SQL and its own query builder (AST Builder). Unlike other ORMs, GlueSQL's AST Builder allows developers to build queries directly with GlueSQL's AST, enabling the use of all of GlueSQL's features. This is why we named it AST Builder instead of Query Builder.

### SQL Example
```sql
SELECT id, name FROM Foo WHERE name = 'Lemon' AND price > 100
```

### AST Builder Example
```rust
table("Foo")
    .select()
    // Filter by name using a SQL string
    .filter("name = 'Lemon'")
    // Filter by price using AST Builder methods
    .filter(col("price").gt(100))
    .project("id, name")
    .execute(glue)
    .await;
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
GlueSQL provides a variety of reference storages out of the box, including simple in-memory storage, key-value databases, log file-based storage like JSON & JSONL, and even Web Storage and IndexedDB supported by web browsers. These reference storages are readily available for use and can be easily adapted to a variety of storage systems. Additionally, GlueSQL is constantly expanding its list of supported storages, making it a versatile tool for developers.

### Memory Storage
Memory Storage is a foundational storage option designed for in-memory, non-persistent data. It is a simple yet robust storage option that can be used in production environments.

### Shared Memory Storage
Shared Memory Storage is a storage option designed to provide more comfortable usage of Memory Storage in concurrent environments. It wraps the Memory Storage with a read-write lock and an atomic reference count, allowing you to clone the storage instance and use it effortlessly across multiple threads. All storage instances will refer to the same data, making it a convenient option for concurrent environments.

### Sled Storage
Sled Storage is a persistent data storage option for GlueSQL that is built on the Sled key-value embedded database in Rust. It is the only storage option currently supported by GlueSQL that implements all Store traits, from non-clustered indexes to transactions. Sled Storage is an excellent choice for handling and storing data in a Rust environment. To use Sled Storage, you can create a SledStorage instance using a path.

### JSON Storage
With GlueSQL, you can use JSONL or JSON files as a database that supports SQL and AST Builder, making it a powerful option for developers who need to work with JSON data. JSON Storage is a storage system that uses two types of files: a schema file (optional) and a data file. The schema file is written in Standard SQL and stores the structure of the table, while the data file contains the actual data and supports two file formats: *.json and *.jsonl. JSON Storage supports all DML features, but is particularly specialized for SELECT and INSERT.

### Web Storage
WebStorage, specifically localStorage and sessionStorage, can be used as a data storage system for GlueSQL. While WebStorage is a simple key-value database that uses string keys, GlueSQL makes it more powerful by adding support for SQL queries. This allows you to use SQL to interact with WebStorage, making it a convenient option for developers who are familiar with SQL. WebStorage can be used in JavaScript (Web) environments and Rust WebAssembly environments.

### IndexedDB Storage
IndexedDB Storage is a powerful storage system that allows you to interact with IndexedDB using SQL. While using IndexedDB directly can be challenging, GlueSQL makes it easy to use by handling version management internally and storing data in JSON format. With GlueSQL, you can use SQL to interact with IndexedDB, making it a convenient option for developers who are familiar with SQL. You can use IndexedDB Storage in both JavaScript (Web) and Rust WebAssembly environments.

### Composite Storage
Composite Storage is a powerful feature of GlueSQL that allows you to bundle together multiple existing storages, enabling you to perform JOIN operations across two distinct storages. This feature is utilized in various environments, including GlueSQL's JavaScript (Web) interface. Specifically, GlueSQL bundles together memory, localStorage, sessionStorage, and IndexedDB using Composite Storage in its JavaScript (Web) interface. This allows you to create tables using four different storages and perform operations like JOIN using SQL, all at once. Composite Storage is a versatile feature that can be used in many different scenarios, making it a valuable tool for developers who need to work with multiple storage systems, including those that require data migration between different storage systems.

## Adapting GlueSQL to Your Environment: Creating Custom Storage
GlueSQL is designed to be adaptable to a wide variety of environments, including file systems, key-value databases, complex NoSQL databases, and remote APIs. To create a custom storage for GlueSQL, you only need to implement the Store and StoreMut traits provided by GlueSQL. These traits allow you to support SELECT queries and modify data, such as INSERT, UPDATE, and DELETE.

If you want to support additional features, such as schema changes, transactions, or custom functions, you can implement the corresponding traits. However, these traits are optional, and you can choose to implement only the ones that are relevant to your storage system.

To make it even easier to develop custom storages, GlueSQL provides a Test Suite that allows you to test your storage implementation against a set of standard SQL queries. This ensures that your storage system is compatible with GlueSQL and can handle common SQL operations.

Overall, creating a custom storage for GlueSQL is a straightforward process that allows you to adapt SQL and the AST Builder to your environment with ease.

## GlueSQL Custom Storage: Let Us Handle It for You
Although anyone can develop a custom storage for GlueSQL with ease, our GlueSQL team can also implement and maintain it for you. This is especially recommended for NoSQL databases with their own query planner and execution layer, as adapting GlueSQL to them requires a deep understanding of GlueSQL's planner and storage layer details. We welcome not only database companies but also any services that want to support SQL and GlueSQL query interfaces. As GlueSQL is rapidly adding and improving features, we can help you develop and manage your custom storage effectively if you entrust it to us. If you're interested, please contact us at <taehoon@gluesql.com>.

## Contribution
GlueSQL is a database project that is simpler than you might think. You only need to know three common Rust project commands: `cargo fmt`, `cargo clippy`, and `cargo test`. Don't hesitate to make pull requests and change the code as you see fit. We have set up GitHub Actions to validate your changes, so you don't have to worry about making mistakes. The line coverage of GlueSQL's core code is almost 99%, which is the result of not only careful test writing, but also of making the test suite easy to understand and use for anyone, even those who are not familiar with Rust. If you're not sure where to start, we recommend exploring the test suite first. Take a look at the existing features and try to understand how they work. Even if you're not familiar with Rust, you should be able to navigate the test suite without any problems. If there's a feature you'd like to see but isn't there yet, implementing it yourself and contributing it to GlueSQL is a great way to get involved. You can also check out the issues on the GlueSQL GitHub repository for more ideas on how to contribute.

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](https://raw.githubusercontent.com/gluesql/gluesql/main/LICENSE) file for details.