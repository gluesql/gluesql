---
sidebar_position: 5
---

# Deleting Data

In this section, we will discuss how to delete data from a table using GlueSQL.

## Delete with Filter

To delete specific rows from a table, you can use the `delete` method on a table object, followed by the `filter` method to provide a condition that the rows must meet. You can then use the `execute` method to apply the changes.

```rust
let actual = table("Foo")
    .delete()
    .filter(col("flag").eq(false))
    .execute(glue)
    .await;
let expected = Ok(Payload::Delete(1));
test(actual, expected);
```

This code deletes the rows in the table `Foo` where the `flag` column value is false.

## Delete All Rows

To delete all rows from a table, you can use the `delete` method on a table object, followed by the `execute` method.

```rust
let actual = table("Foo").delete().execute(glue).await;
let expected = Ok(Payload::Delete(2));
test(actual, expected);
```

This code deletes all rows from the table `Foo`.
