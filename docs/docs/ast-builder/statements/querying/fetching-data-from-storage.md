---
sidebar_position: 1
---

# Fetching Data from Storage

The AST Builder provides a powerful and flexible way to query data from your tables, similar to SQL's SELECT statement. This guide will show you how to use the AST Builder's `table("foo").select()` method to perform various query types, including filtering, joining, grouping, ordering, and pagination.

## Basic SELECT

To perform a basic SELECT query using the AST Builder, simply call the `select()` method on a table object.

```rust
let actual = table("Category").select().execute(glue).await;
```

## Filtering (WHERE)

To filter the results of a SELECT query, use the `filter()` method, providing a condition as a string.

```rust
let actual = table("Category")
    .select()
    .filter("name = 'Meat'")
    .execute(glue)
    .await;
```

## Joining Tables

You can join tables using the `join()` or `join_as()` methods. The following example demonstrates an INNER JOIN:

```rust
let actual = table("Item")
    .alias_as("i")
    .select()
    .join_as("Category", "c")
    .on("c.id = i.category_id")
    .filter("c.name = 'Fruit' OR c.name = 'Meat'")
    .project("i.name AS item")
    .project("c.name AS category")
    .execute(glue)
    .await;
```

For LEFT OUTER JOIN, use the `left_join()` method:

```rust
let actual = table("Category")
    .select()
    .left_join("Item")
    .on(col("Category.id")
        .eq(col("Item.category_id"))
        .and(col("price").gt(50)))
    .project(vec![
        "Category.name AS category",
        "Item.name AS item",
        "price",
    ])
    .execute(glue)
    .await;
```

## Grouping and Aggregating (GROUP BY, HAVING)

To group the results of a SELECT query, use the `group_by()` method. You can also filter the groups using the `having()` method.

```rust
let actual = table("Item")
    .select()
    .join("Category")
    .on(col("Category.id").eq("Item.category_id"))
    .group_by("Item.category_id")
    .having("SUM(Item.price) > 80")
    .project("Category.name AS category")
    .project("SUM(Item.price) AS sum_price")
    .execute(glue)
    .await;
```

## Sorting Results (ORDER BY)

To sort the results of a SELECT query, use the `order_by()` method.

```rust
let actual = table("Item")
    .select()
    .project("name, price")
    .order_by("price DESC")
    .execute(glue)
    .await;
```

You can also use typed expression helpers:

```rust
let actual = table("Item")
    .select()
    .project("name, price")
    .order_by(col("price").desc())
    .execute(glue)
    .await;
```

## Pagination (OFFSET, LIMIT)

You can paginate the results of a SELECT query using the `offset()` and `limit()` methods.

```rust
let actual = table("Item")
    .select()
    .project("name, price")
    .order_by("price DESC")
    .offset(1)
    .limit(2)
    .execute(glue)
    .await;
```
