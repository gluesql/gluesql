---
sidebar_position: 1
---

# BOOLEAN

The `BOOLEAN` data type in SQL is used to store boolean values, which can be either `TRUE` or `FALSE`. This data type is useful for representing binary states or conditions in your data.

Here's an example of how to create a table, insert data, and query data using the `BOOLEAN` data type:

## Creating a table with a BOOLEAN column

To create a table with a BOOLEAN column, use the following SQL syntax:

```sql
CREATE TABLE user_active (username TEXT, is_active BOOLEAN);
```

## Inserting data into the BOOLEAN column

To insert data into the BOOLEAN column, provide the boolean values as `TRUE` or `FALSE`:

```sql
INSERT INTO user_active (username, is_active) VALUES
    ('user1', TRUE),
    ('user2', FALSE),
    ('user3', TRUE);
```

## Querying data from the BOOLEAN column

To query data from the BOOLEAN column, use standard SQL syntax:

```sql
SELECT username, is_active FROM user_active;
```

This query will return the following result:

```
username | is_active
---------|----------
user1    | TRUE
user2    | FALSE
user3    | TRUE
```

## Casting between BOOLEAN and INTEGER

You can cast between BOOLEAN and INTEGER values:

- When casting a BOOLEAN to an INTEGER, `TRUE` becomes `1` and `FALSE` becomes `0`.
- When casting an INTEGER to a BOOLEAN, `1` becomes `TRUE` and `0` becomes `FALSE`. Other integer values will result in an error.

Example:

```sql
SELECT CAST(1 AS BOOLEAN); -- Result: TRUE
SELECT CAST(0 AS BOOLEAN); -- Result: FALSE
SELECT CAST(TRUE AS INTEGER); -- Result: 1
SELECT CAST(FALSE AS INTEGER); -- Result: 0
```

Note that casting negative integers or integers greater than 1 to BOOLEAN will result in an error.

## Conclusion

In summary, the `BOOLEAN` data type is a simple yet powerful way to represent binary states in SQL databases. With its ability to store `TRUE` and `FALSE` values, it can be used in various applications where binary conditions are necessary. Additionally, its compatibility with casting to and from INTEGER values provides added flexibility in data manipulation and querying. By understanding the basics of the BOOLEAN data type and its use cases, you can effectively use it in your database designs and operations.