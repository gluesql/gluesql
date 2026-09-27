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
    .execute(glue);
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
    .execute(glue);
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
    .execute(glue);
let expected = Ok(Payload::Insert(3));
test(actual, expected);
```

This code inserts data into the table `Bar` using the `SELECT` statement on the table `Foo`. The `project` method is used to specify the columns `id` and `name` as the source data.

## Insert from Structs

If you derive `ToGlueRow` on a struct, you can insert struct values directly with the `values_from` method — no manual field-to-expression mapping required. Columns are set automatically from the struct fields, and `#[glue(rename = "...")]` lets a field map to a different column name. `Option` fields convert `None` to `NULL`.

```rust
use gluesql::ToGlueRow;

#[derive(ToGlueRow)]
struct Item {
    id: i64,
    #[glue(rename = "name")]
    title: String,
    rate: Option<f64>, // None -> NULL
}

let items = vec![
    Item { id: 4, title: "Fish".to_owned(), rate: Some(0.2) },
    Item { id: 5, title: "Bread".to_owned(), rate: None },
];

let actual = table("Foo")
    .insert()
    .values_from(&items)?
    .execute(glue);
let expected = Ok(Payload::Insert(2));
test(actual, expected);
```

`values_from` returns an error if the given slice is empty, and it always sets the insert columns from the struct metadata so the columns stay aligned with the generated values.
