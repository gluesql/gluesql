---
sidebar_position: 7
---

# TIMESTAMP

In GlueSQL, the `TIMESTAMP` data type is used to store date and time values in the format 'YYYY-MM-DD HH:MM:SS.SSSS'. Although timezone information can be included in the input string, GlueSQL stores all `TIMESTAMP` values in UTC, discarding the timezone information.

## Creating a table with TIMESTAMP columns

To create a table with columns of type `TIMESTAMP`, use the `CREATE TABLE` statement:

```sql
CREATE TABLE TimestampLog (
    id INTEGER,
    t1 TIMESTAMP,
    t2 TIMESTAMP
);
```

## Inserting data into a table with TIMESTAMP columns

To insert data into a table with `TIMESTAMP` columns, use the `INSERT INTO` statement:

```sql
INSERT INTO TimestampLog VALUES
    (1, '2020-06-11 11:23:11Z',           '2021-03-01'),
    (2, '2020-09-30 12:00:00 -07:00',     '1989-01-01T00:01:00+09:00'),
    (3, '2021-04-30T07:00:00.1234-17:00', '2021-05-01T09:00:00.1234+09:00');
```

The input strings include timezone information, but GlueSQL will convert and store them as UTC timestamps.

## Querying data from a table with TIMESTAMP columns

To query data from a table with `TIMESTAMP` columns, use the `SELECT` statement:

```sql
SELECT id, t1, t2 FROM TimestampLog;
```

## Filtering data using TIMESTAMP columns

You can use various comparison operators like `>`, `<`, `<=`, `>=`, and `=` to filter data based on `TIMESTAMP` columns:

```sql
SELECT * FROM TimestampLog WHERE t1 > t2;

SELECT * FROM TimestampLog WHERE t1 = t2;

SELECT * FROM TimestampLog WHERE t1 = '2020-06-11T14:23:11+0300';

SELECT * FROM TimestampLog WHERE t2 < TIMESTAMP '2000-01-01';
```

## Performing timestamp arithmetic

You can perform arithmetic operations on `TIMESTAMP` columns using `INTERVAL`:

```sql
SELECT id, t1 - t2 AS timestamp_sub FROM TimestampLog;

SELECT
    id,
    t1 - INTERVAL '1' DAY AS sub,
    t2 + INTERVAL '1' MONTH AS add
FROM TimestampLog;
```

## Handling invalid timestamp values

If you try to insert an invalid timestamp value into a `TIMESTAMP` column, GlueSQL will return an error:

```sql
INSERT INTO TimestampLog VALUES (1, '12345-678', '2021-05-01');
```

This will result in an error similar to the following:

```
failed to parse timestamp: 12345-678
```

## Conclusion

In GlueSQL, the TIMESTAMP data type allows you to store date and time values with precision up to milliseconds. The provided code snippet demonstrates how to create a table with TIMESTAMP columns, insert data into it, and perform various queries and operations on the data. When inserting a TIMESTAMP value, the timezone information is removed, and the data is stored in UTC. This ensures that all time values are consistent and can be easily converted to different time zones when needed.