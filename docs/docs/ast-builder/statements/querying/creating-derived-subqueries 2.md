---
sidebar_position: 3
---

# Creating Derived Subqueries

This document covers the `alias_as` functionality of the AST Builder in the GlueSQL project. The `alias_as` method allows you to create a derived subquery, which is similar to subqueries in SQL. It gives you the ability to use the output of a query as a table to perform further queries.

## Basic Usage

To use the `alias_as` method, simply chain it to the end of a query builder method before executing the query. The derived subquery can then be used for further queries. Here's an example:

```rust
let actual = table("Item")
    .select()
    .alias_as("Sub")
    .select()
    .execute(glue)
    .await;
```

In this example, the `alias_as` method is used after the `select` method, creating a derived subquery named "Sub" that can be used in subsequent queries.

## Examples

The following examples demonstrate how to use the `alias_as` method with various query operations. 

### Derived Subquery with Filter

```rust
let actual = table("Item")
    .select()
    .filter("item_id = 300")
    .alias_as("Sub")
    .select()
    .execute(glue)
    .await;
```

### Derived Subquery with Projection

```rust
let actual = table("Item")
    .select()
    .project("item_id")
    .alias_as("Sub")
    .select()
    .execute(glue)
    .await;
```

### Derived Subquery with Join

```rust
let actual = table("Item")
    .alias_as("i")
    .select()
    .join_as("Category", "c")
    .on("c.category_id = i.category_id")
    .alias_as("Sub")
    .select()
    .project("item_name")
    .project("category_name")
    .execute(glue)
    .await;
```

### Derived Subquery with Group By and Having

```rust
let actual = table("Category")
    .select()
    .project("category_name")
    .alias_as("Sub1")
    .select()
    .group_by("category_name")
    .having("category_name = 'Meat'")
    .alias_as("Sub2")
    .select()
    .execute(glue)
    .await;
```

### Derived Subquery with Order By

```rust
let actual = table("Item")
    .select()
    .order_by("price DESC")
    .alias_as("Sub")
    .select()
    .execute(glue)
    .await;
```

### Derived Subquery with Offset and Limit

This example shows how to create a derived subquery combined with both the `offset` and `limit` methods to control the range of rows returned:

```rust
let actual = table("Item")
    .select()
    .offset(3)
    .limit(1)
    .alias_as("Sub")
    .select()
    .execute(glue)
    .await;
```