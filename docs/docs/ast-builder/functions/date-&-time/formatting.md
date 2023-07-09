# Formatting

In GlueSQL, you can format date, time, and timestamp values to a specific format using the `format` function.

For this tutorial, we assume there's a table named `Visitor` with columns `id`, `name`, `visit_date`, `visit_time`, and `visit_timestamp`.

## Formatting Date

The `format` function can be used to change the format of a date. 

```rust
let actual = table("Visitor")
    .select()
    .project("name")
    .project("visit_date")
    .project(col("visit_date").format(text("%Y-%m")))  // Formats the visit_date to the year-month format
    .project(format(col("visit_date"), text("%m")))  // Formats the visit_date to the month format
    .execute(glue)
    .await;
```

## Formatting Time

The `format` function can also be used to change the format of a time.

```rust
let actual = table("Visitor")
    .select()
    .project("name")
    .project("visit_time")
    .project(col("visit_time").format(text("%H:%M:%S")))  // Formats the visit_time to the hour-minute-second format
    .project(format(col("visit_time"), text("%M:%S")))  // Formats the visit_time to the minute-second format
    .execute(glue)
    .await;
```

## Formatting Timestamp

The `format` function can be used to change the format of a timestamp. 

```rust
let actual = table("Visitor")
    .select()
    .project("name")
    .project("visit_timestamp")
    .project(col("visit_timestamp").format(text("%Y-%m-%d %H:%M:%S")))  // Formats the visit_timestamp to the year-month-date hour-minute-second format
    .project(format(col("visit_timestamp"), text("%Y-%m-%d %H:%M:%S")))  // Formats the visit_timestamp to the year-month-date hour-minute-second format
    .execute(glue)
    .await;
```

