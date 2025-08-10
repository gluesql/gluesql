# Current Date and Time

GlueSQL provides a function to get the current date and time: `now`.

## Now - now

The `now` function returns the current date and time.

```rust
let actual = table("Record")
    .select()
    .filter(col("time_stamp").gt(now()))  // select rows where "time_stamp" is later than current time
    .project("id, time_stamp")
    .execute(glue)
    .await;
```

In the above example, the `filter` method uses `now` to select rows where the "time_stamp" column is later than the current time.

When inserting data into a table, you can use the `now` function to record the current time:

```rust
let actual = table("Record")
    .insert()
    .values(vec![
        "1, '2022-12-23T05:30:11.164932863'",
        "2, NOW()",  // Inserts the current time
        "3, '9999-12-31T23:59:40.364832862'",
    ])
    .execute(glue)
    .await;
```
In the example above, the "time_stamp" column for the row with id 2 is set to the current time at the moment of insertion.