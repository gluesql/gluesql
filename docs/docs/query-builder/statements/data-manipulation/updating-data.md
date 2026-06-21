---
sidebar_position: 4
---

# Updating Data

In this section, we will discuss how to update data in a table using GlueSQL.

## Basic Update

To update data in a table, you can use the `update` method on a table object, followed by the `set` method to specify the column and the new value. You can then use the `execute` method to apply the changes.

```rust
let actual = table("Foo")
    .update()
    .set("score", col("score").div(10))
    .execute(glue)
    .await;
let expected = Ok(Payload::Update(3));
test(actual, expected);
```

This code updates all rows in the table `Foo`, dividing the `score` column value by 10.

## Update with Multiple Columns

To update multiple columns, you can chain multiple `set` methods with the desired column names and new values.

```rust
let actual = table("Foo")
    .update()
    .set("score", "score * 2 + 5")
    .set("flag", col("flag").negate())
    .execute(glue)
    .await;
let expected = Ok(Payload::Update(3));
test(actual, expected);
```

This code updates all rows in the table `Foo`, applying the following changes:
1. The `score` column value is multiplied by 2 and 5 is added.
2. The `flag` column value is negated (i.e., true becomes false and false becomes true).

## Update with Filter

If you want to update only specific rows, you can use the `filter` method to provide a condition that the rows must meet.

```rust
let actual = table("Foo")
    .update()
    .set("score", "score * 2 + 5")
    .set("flag", col("flag").negate())
    .filter(col("score").lte(30))
    .execute(glue)
    .await;
let expected = Ok(Payload::Update(2));
test(actual, expected);
```

This code updates the rows in the table `Foo` where the `score` column value is less than or equal to 30. The `score` and `flag` column values are updated as described in the previous example.