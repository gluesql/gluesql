# Trimming

GlueSQL provides several text trimming functions that allow you to remove leading or trailing characters from a text string.

For this tutorial, we assume there's a table named `Food` with an `id` column of `INTEGER` type and a `name` column of `TEXT` type.

## Right Trimming - rtrim

The `rtrim` function removes trailing characters from a text string. You can specify the characters to be removed as an argument to the function. If no argument is provided, it trims spaces by default.

```rust
// Trims trailing spaces from "chicken   "
let test_text = text("chicken   ").rtrim(Some(text(" ")));

let actual = table("Food")
    .insert()
    .columns("id, name")
    .values(vec![vec![num(1), test_text]])
    .execute(glue)
    .await;
```

## Left Trimming - ltrim

The `ltrim` function removes leading characters from a text string. You can specify the characters to be removed as an argument to the function. If no argument is provided, it trims spaces by default.

```rust
// Trims leading spaces from "   chicken"
let test_text = ltrim(text("   chicken"), Some(text(" ")));

let actual = table("Food")
    .insert()
    .columns("id, name")
    .values(vec![vec![num(2), test_text]])
    .execute(glue)
    .await;
```

## Right and Left Trimming

You can combine `rtrim` and `ltrim` to trim both sides of a string:

```rust
// Trims leading "ch" and trailing spaces from "chicken"
let test_text = text("chicken").ltrim(Some(text("ch"))).rtrim(None);

let actual = table("Food")
    .insert()
    .columns("id, name")
    .values(vec![vec![num(3), test_text]])
    .execute(glue)
    .await;
```

```rust
// Trims trailing "en" and leading spaces from "chicken"
let test_text = text("chicken").rtrim(Some(text("en"))).ltrim(None);

let actual = table("Food")
    .insert()
    .columns("id, name")
    .values(vec![vec![num(4), test_text]])
    .execute(glue)
    .await;
```
