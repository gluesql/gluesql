---
sidebar_position: 2
---

# Using Preloaded Data

This guide will show you how to use AST Builder to query data that has already been loaded into memory, as opposed to querying data from storage. This is similar to SQL's `VALUES` functionality. 

## Creating a Values Object

To create a `values()` object, you can either provide a vector of strings or a vector of vectors of strings. Each inner vector represents a row of data, and each string within the inner vector represents a value in that row.

```rust
use gluesql_core::ast_builder::values;

let actual = values(vec!["1, 'Glue'", "2, 'SQL'", "3, 'Rust'"])
    .execute(glue)
    .await;

let actual = values(vec![
    vec!["1", "'Glue'"],
    vec!["2", "'SQL'"],
    vec!["3", "'Rust'"],
])
.execute(glue)
.await;
```

## Sorting Results (ORDER BY)

To sort the results of a `values()` query, use the `order_by()` method.

```rust
let actual = values(vec!["1, 'Glue'", "2, 'SQL'", "3, 'Rust'"])
    .order_by("column2 desc")
    .execute(glue)
    .await;
```

## Pagination (OFFSET, LIMIT)

You can paginate the results of a `values()` query using the `offset()` and `limit()` methods.

```rust
let actual = values(vec!["1, 'Glue'", "2, 'SQL'", "3, 'Rust'"])
    .offset(1)
    .execute(glue)
    .await;

let actual = values(vec!["1, 'Glue'", "2, 'SQL'", "3, 'Rust'"])
    .limit(2)
    .execute(glue)
    .await;
```

## Querying Preloaded Data

To query preloaded data using the `values()` object, you can call the `select()` method, and then use the `project()` method to specify the columns you want to include in the result.

```rust
let actual = values(vec!["1, 'Glue'", "2, 'SQL'", "3, 'Rust'"])
    .alias_as("Sub")
    .select()
    .project("column1 AS id")
    .project("column2 AS name")
    .execute(glue)
    .await;
```