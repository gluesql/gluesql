# GlueSQL

[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![npm](https://img.shields.io/npm/v/gluesql?color=red)](https://www.npmjs.com/package/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![Chat](https://img.shields.io/discord/780298017940176946?logo=discord&logoColor=white)](https://discord.gg/C6TDEgzDzY)
[![Coverage Status](https://coveralls.io/repos/github/gluesql/gluesql/badge.svg?branch=main)](https://coveralls.io/github/gluesql/gluesql?branch=main)

## Multi-Model Database Engine as a Library
GlueSQL is a SQL database library written in Rust that provides a parser ([sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)), execution layer, and a range of storage options, including both persistent and non-persistent storage engines, packaged into a single library.
It supports both SQL and its own query builder (AST Builder), making it a versatile tool for developers. GlueSQL can handle structured and unstructured data, making it suitable for a wide range of use cases.
It is portable and can be used with various storages, including log files and read-write capable storage. GlueSQL is designed to be extensible and supports custom planners, making it a powerful tool for developers who need SQL support for their databases or services.
GlueSQL can be used in Rust and JavaScript environments, making it a flexible choice for developers working in different languages.

For more information on how to use GlueSQL, please refer to the [**official documentation website**](https://gluesql.org/docs). The documentation provides detailed information on how to install and use GlueSQL, as well as examples and tutorials on how to create custom storage systems and perform SQL operations.

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

### SQL Example
```sql
CREATE TABLE Foo (id INTEGER);
INSERT INTO Foo VALUES (1), (2);

CREATE TABLE Bar;
INSERT INTO Bar VALUES
    ('{ "name": "glue", "value": 30 }'),
    ('{ "name": "sql", "rate": 3.0, "list": [1, 2, 3] }');

SELECT * FROM Foo JOIN Bar
WHERE Bar.rate > 1.0 AND Foo.id = 2;
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
JSON Storage is a storage system that consists of two types of files: a schema file (optional) and a data file. The schema file is written in Standard SQL and stores the structure of the table, while the data file contains the actual data and supports two file formats: *.json and *.jsonl. JSON Storage supports all DML features, but is particularly specialized for SELECT and INSERT.

### Web Storage
WebStorage, specifically localStorage and sessionStorage, can be used as a data storage system for GlueSQL. It supports READ and WRITE operations and provides a simple and easy-to-use interface for reading and writing data using a string key. WebStorage can be used in JavaScript (Web) environments and Rust WebAssembly environments.

### IndexedDB Storage
IndexedDB Storage is a storage system that allows you to interact with IndexedDB using SQL. GlueSQL handles version management internally and stores data in JSON format. You can use it in both JavaScript (Web) and Rust WebAssembly environments.

### Composite Storage
Composite Storage is a special type of storage that allows you to bundle together multiple existing storages, enabling you to perform JOIN operations across two distinct storages. GlueSQL utilizes Composite Storage in its JavaScript (Web) interface, allowing you to create tables using four different storages and perform operations like JOIN using SQL.

## Adapting GlueSQL to Your Environment: Creating Custom Storage
GlueSQL is designed to be adaptable to a wide variety of environments, including file systems, key-value databases, complex NoSQL databases, and remote APIs. To create a custom storage for GlueSQL, you only need to implement the Store and StoreMut traits provided by GlueSQL. These traits allow you to support SELECT queries and modify data, such as INSERT, UPDATE, and DELETE.

If you want to support additional features, such as schema changes, transactions, or custom functions, you can implement the corresponding traits. However, these traits are optional, and you can choose to implement only the ones that are relevant to your storage system.

To make it even easier to develop custom storages, GlueSQL provides a Test Suite that allows you to test your storage implementation against a set of standard SQL queries. This ensures that your storage system is compatible with GlueSQL and can handle common SQL operations.

Overall, creating a custom storage for GlueSQL is a straightforward process that allows you to adapt SQL and the AST Builder to your environment with ease.

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.