# Case Conversion 

GlueSQL provides several text case conversion functions that allow you to convert text data to upper case, lower case or capitalize each word in a string.

For this tutorial, we assume there's a table named `Item` with various columns including `name`, `opt_name`, and `capped_name` which are of `TEXT` type.

## Upper Case Conversion - upper

The `upper` function converts a text string to upper case.

```rust
let actual = table("Item")
    .select()
    .project(col("name").upper())  // Convert the 'name' column to upper case
    .execute(glue)
    .await;
```

## Lower Case Conversion - lower

The `lower` function converts a text string to lower case.

```rust
let actual = table("Item")
    .select()
    .project(col("name").lower())  // Convert the 'name' column to lower case
    .execute(glue)
    .await;
```

You can also filter the records based on the lower case conversion:

```rust
let actual = table("Item")
    .select()
    .filter(col("name").lower().eq("'abcd'"))  // Filter records where lower case of 'name' is 'abcd'
    .project("name")
    .project(lower("name"))
    .execute(glue)
    .await;
```

## Initial Capital Case Conversion - initcap

The `initcap` function converts a text string to initial capital case, i.e., it capitalizes the first character of each word in the string.

```rust
let actual = table("Item")
    .select()
    .project(col("capped_name").initcap())  // Convert the 'capped_name' column to initial capital case
    .execute(glue)
    .await;
```

You can also filter the records based on the initial capital case conversion:

```rust
let actual = table("Item")
    .select()
    .filter(col("capped_name").initcap().eq("'H/I Jk'"))  // Filter records where initial capital case of 'capped_name' is 'H/I Jk'
    .project("capped_name")
    .execute(glue)
    .await;
```