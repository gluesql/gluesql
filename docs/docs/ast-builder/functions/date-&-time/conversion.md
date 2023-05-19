# Conversion

GlueSQL provides date and time conversion functions that allow you to convert text data to datetime data types such as Date, Time, and Timestamp. These functions are `to_date`, `to_time`, and `to_timestamp`.

For this tutorial, we assume there's a table named `Visitor` with various columns including `visit_date`, `visit_time`, and `visit_time_stamp` which are of `TEXT` type.

## Date Conversion - to_date

The `to_date` function converts a text string to a date.

There are two ways to call the `to_date` function in GlueSQL:

```rust
let actual = table("Visitor")
    .select()
    .project("id")
    .project("name")
    .project(col("visit_date").to_date("'%Y-%m-%d'"))  // Method 1: Calling the to_date method on a column
    .project(to_date("visit_date", "'%Y-%m-%d'"))  // Method 2: Using the to_date function directly
    .execute(glue)
    .await;
```

## Time Conversion - to_time

The `to_time` function converts a text string to a time.

There are two ways to call the `to_time` function in GlueSQL:

```rust
let actual = table("Visitor")
    .select()
    .project("id")
    .project("name")
    .project(col("visit_time").to_time("'%H:%M:%S'"))  // Method 1: Calling the to_time method on a column
    .project(to_time("visit_time", "'%H:%M:%S'"))  // Method 2: Using the to_time function directly
    .execute(glue)
    .await;
```

## Timestamp Conversion - to_timestamp

The `to_timestamp` function converts a text string to a timestamp.

There are two ways to call the `to_timestamp` function in GlueSQL:

```rust
let actual = table("Visitor")
    .select()
    .project("id")
    .project("name")
    .project(col("visit_time_stamp").to_timestamp("'%Y-%m-%d %H:%M:%S'"))  // Method 1: Calling the to_timestamp method on a column
    .project(to_timestamp("visit_time_stamp", "'%Y-%m-%d %H:%M:%S'"))  // Method 2: Using the to_timestamp function directly
    .execute(glue)
    .await;
```
