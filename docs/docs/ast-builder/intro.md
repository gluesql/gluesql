---
title: Introduction
sidebar_position: 1
---

# AST Builder

GlueSQL offers two ways to create and execute queries: using SQL statements or using the AST Builder. In this introductory page, we will focus on the AST Builder.

When executing SQL statements in GlueSQL, they need to be converted into an internal AST (Abstract Syntax Tree) format. The AST Builder allows users to directly create and manipulate ASTs, making it more efficient and flexible compared to traditional SQL statements.

The AST Builder has some similarities to ORM (Object Relational Mapping) query builders, but there are several key differences:
- ORM query builders often support multiple databases, which can limit their features to a subset of each database's capabilities. However, the AST Builder is designed exclusively for GlueSQL, allowing it to take full advantage of all GlueSQL features.
- The AST Builder is flexible in terms of input, accepting both its own API calls and SQL expressions.
- ORM query builders typically generate SQL statements, which must then be executed by the database. This introduces overhead. In contrast, the GlueSQL AST Builder directly generates executable ASTs, making it highly efficient.
- The AST Builder supports features that are not available with SQL, such as allowing users to directly specify the internal execution strategy. This is similar to SQL query hints, but with the AST Builder, the user's instructions are executed precisely, rather than being treated as suggestions.

Currently, the AST Builder only supports Rust language interfaces, but support for other languages, such as JavaScript, is planned for future releases.

Below are some sample code snippets using the GlueSQL AST Builder in Rust, categorized by query type:

### CREATE TABLE
```rust
let actual = table("Foo")
    .create_table()
    .add_column("id INTEGER")
    .add_column("name TEXT")
    .execute(glue)
    .await;
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
    .execute(glue)
    .await;
```

### SELECT
```rust
let actual = table("Foo")
    .select()
    .project("id, name")
    .execute(glue)
    .await;
```

### UPDATE
```rust
let actual = table("Foo")
    .update()
    .set("id", col("id").mul(2))
    .filter(col("id").eq(200))
    .execute(glue)
    .await;
```

### SELECT with filtering
```rust
let actual = table("Foo")
    .select()
    .filter("name = 'Lemon'")
    .project("id, name")
    .build()
    .expect("build and execute")
    .execute(glue)
    .await;
```

### DELETE
```rust
let actual = table("Foo")
    .delete()
    .filter(col("id").gt(200))
    .execute(glue)
    .await;
```

## Summary

In this introduction to the AST Builder, we have covered the key differences between the AST Builder and ORM query builders, and provided examples of how to use the AST Builder in Rust for various query types. The AST Builder is a powerful and efficient tool for working with GlueSQL, offering greater flexibility and control compared to traditional SQL statements.

Remember that the AST Builder currently supports only Rust language interfaces, but support for other languages, such as JavaScript, is planned for future releases.

By leveraging the AST Builder, you can take full advantage of GlueSQL's features, and build more efficient and flexible database applications.