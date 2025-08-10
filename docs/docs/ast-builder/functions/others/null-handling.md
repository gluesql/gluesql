# Null Handling 

In some cases, you may need to handle `NULL` values in your database. GlueSQL provides functions called `ifnull` and `nullif` to handle these cases.

## IFNULL - ifnull

The `ifnull` function checks if the first expression is `NULL`, and if it is, it returns the value of the second expression. If the first expression is not `NULL`, it returns the value of the first expression.

```rust
let actual = table("Foo")
    .select()
    .project("id")
    .project(col("name").ifnull(text("isnull")))  // If the "name" column is NULL, replace it with "isnull"
    .execute(glue)
    .await;
```

In the above example, if the "name" column is `NULL`, "isnull" is returned. Otherwise, the value of the "name" column is returned.

You can also use `ifnull` with another column:

```rust
let actual = table("Foo")
    .select()
    .project("id")
    .project(col("name").ifnull(col("nickname")))  // If the "name" column is NULL, replace it with the value from the "nickname" column
    .execute(glue)
    .await;
```

In this example, if the "name" column is `NULL`, the value from the "nickname" column is returned. If "name" is not `NULL`, the value of the "name" column is returned.

The `ifnull` function can also be used without a table:

```rust
let actual = values(vec![
    vec![ast_builder::ifnull(text("HELLO"), text("WORLD"))],  // If "HELLO" is NULL (it's not), return "WORLD". Otherwise, return "HELLO".
    vec![ast_builder::ifnull(null(), text("WORLD"))],  // If NULL is NULL (it is), return "WORLD".
])
.execute(glue)
.await;
```

In the first case, "HELLO" is returned because it's not `NULL`. In the second case, "WORLD" is returned because the first value is `NULL`.