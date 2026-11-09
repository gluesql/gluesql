---
title: Introduction
sidebar_position: 1
---

# Query Builder

GlueSQL offers two ways to create and execute queries: using SQL statements or using the Query Builder. In this introductory page, we will focus on the Query Builder.

When executing SQL statements in GlueSQL, they are parsed into an AST (Abstract Syntax Tree) and translated into an execution-facing statement plan. The Query Builder lets users construct those query and statement plans programmatically without generating SQL strings first. Explicit AST construction methods remain available where AST output is still needed.

The Query Builder has some similarities to ORM (Object Relational Mapping) query builders, but there are several key differences:
- ORM query builders often support multiple databases, which can limit their features to a subset of each database's capabilities. However, the Query Builder is designed exclusively for GlueSQL, allowing it to take full advantage of all GlueSQL features.
- The Query Builder is flexible in terms of input, accepting both its own API calls and SQL expressions.
- ORM query builders typically generate SQL statements, which must then be parsed and executed by the database. In contrast, the GlueSQL Query Builder directly builds statement plans for execution.
- The Query Builder supports features that are not available with SQL, such as allowing users to directly specify the internal execution strategy. This is similar to SQL query hints, but with the Query Builder, the user's instructions are executed precisely, rather than being treated as suggestions.

Currently, the Query Builder only supports Rust language interfaces, but support for other languages, such as JavaScript, is planned for future releases.

Below are some sample code snippets using the GlueSQL Query Builder in Rust, categorized by query type:

### CREATE TABLE
```rust
let actual = table("Foo")
    .create_table()
    .add_column("id INTEGER")
    .add_column("name TEXT")
    .execute(glue);
```

### INSERT
```rust
let actual = table("Foo")
    .insert()
    .columns("id, name")
    .values(vec![
        vec![num(100), text("Pickle")],
        vec![num(200), text("Lemon")],
    ])
    .execute(glue);
```

### SELECT
```rust
let actual = table("Foo")
    .select()
    .project("id, name")
    .execute(glue);
```

### UPDATE
```rust
let actual = table("Foo")
    .update()
    .set("id", col("id").mul(2))
    .filter(col("id").eq(200))
    .execute(glue);
```

### SELECT with filtering
```rust
let actual = table("Foo")
    .select()
    .filter("name = 'Lemon'")
    .project("id, name")
    .build()
    .expect("build and execute")
    .execute(glue);
```

### DELETE
```rust
let actual = table("Foo")
    .delete()
    .filter(col("id").gt(200))
    .execute(glue);
```

## Supplying schemas

Queries use the table definitions in storage by default. To supply definitions
yourself, call `with_schema(...)` before `build()` or `execute()`. Each call accepts
either CREATE TABLE DDL or a `gluesql_core::data::Schema`:

```rust
use gluesql_core::{data::Schema, query_builder::{Build, table}};

let source_schema = Schema::from_ddl("CREATE TABLE Source (id INT)")?;
let plan = table("Item")
    .insert()
    .as_select(table("Source").select())
    .with_schema("CREATE TABLE Item (id INT)")
    .with_schema(source_schema)
    .build()?;
```

Supply every table definition the statement needs, including tables involved in
foreign-key checks. Missing definitions are not loaded from storage.

Use the actual table names, even when the query uses aliases. If you supply the
same table more than once, the last definition is used. Supplying a definition
does not create the table.

## Summary

In this introduction to the Query Builder, we have covered the key differences between the Query Builder and ORM query builders, and provided examples of how to use the Query Builder in Rust for various query types. The Query Builder is a powerful and efficient tool for working with GlueSQL, offering greater flexibility and control compared to traditional SQL statements.

Remember that the Query Builder currently supports only Rust language interfaces, but support for other languages, such as JavaScript, is planned for future releases.

By leveraging the Query Builder, you can take full advantage of GlueSQL's features, and build more efficient and flexible database applications.
