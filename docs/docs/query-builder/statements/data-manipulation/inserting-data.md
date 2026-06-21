---
sidebar_position: 3
---

# Inserting Data

In this section, we will discuss how to insert data into a table using GlueSQL.

## Basic Insert

To insert data into a table, you can use the `insert` method on a table object. You can then use the `values` method to provide the values you want to insert.

```rust
let actual = table("Foo")
    .insert()
    .values(vec!["1, 'Fruit', 0.1", "2, 'Meat', 0.8"])
    .execute(glue)
    .await;
let expected = Ok(Payload::Insert(2));
test(actual, expected);
```

This code inserts two rows into the table `Foo`. The first row has the values `1, 'Fruit', 0.1` and the second row has the values `2, 'Meat', 0.8`.

## Insert with Specified Columns

If you want to specify the columns to insert data into, you can use the `columns` method followed by the `values` method. The `values` method should contain the data for the specified columns.

```rust
let actual = table("Foo")
    .insert()
    .columns("id, name")
    .values(vec![vec![num(3), text("Drink")]])
    .execute(glue)
    .await;
let expected = Ok(Payload::Insert(1));
test(actual, expected);
```

This code inserts a new row into the table `Foo` with the specified columns `id` and `name`. The `rate` column is not specified, so it will be set to its default value.

## Insert from Source

You can also insert data into a table using a `SELECT` statement as the source. To do this, use the `as_select` method followed by the `execute` method.

```rust
let actual = table("Bar")
    .insert()
    .as_select(table("Foo").select().project("id, name"))
    .execute(glue)
    .await;
let expected = Ok(Payload::Insert(3));
test(actual, expected);
```

This code inserts data into the table `Bar` using the `SELECT` statement on the table `Foo`. The `project` method is used to specify the columns `id` and `name` as the source data.
