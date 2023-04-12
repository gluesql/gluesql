---
sidebar_position: 8
---

# TIME

In GlueSQL, the `TIME` data type is used to store time values in the format 'HH:MM:SS.SSS'. The code snippet provided demonstrates how to create a table with `TIME` columns, insert data into it, and perform various queries and operations on the data.

## Creating a table with TIME columns

To create a table with columns of type `TIME`, use the `CREATE TABLE` statement:

```sql
CREATE TABLE TimeLog (
    id INTEGER,
    time1 TIME,
    time2 TIME
);
```

## Inserting data into a table with TIME columns

To insert data into a table with `TIME` columns, use the `INSERT INTO` statement:

```sql
INSERT INTO TimeLog VALUES
    (1, '12:30:00', '13:31:01.123'),
    (2, '9:2:1', 'AM 08:02:01.001'),
    (3, 'PM 2:59', '9:00:00 AM');
```

## Querying data from a table with TIME columns

To query data from a table with `TIME` columns, use the `SELECT` statement:

```sql
SELECT id, time1, time2 FROM TimeLog;
```

## Filtering data using TIME columns

You can use various comparison operators like `>`, `<`, `<=`, `>=`, and `=` to filter data based on `TIME` columns:

```sql
SELECT * FROM TimeLog WHERE time1 > time2;

SELECT * FROM TimeLog WHERE time1 <= time2;

SELECT * FROM TimeLog WHERE time1 = TIME '14:59:00';

SELECT * FROM TimeLog WHERE time1 < '1:00 PM';
```

## Performing time arithmetic

You can perform arithmetic operations on `TIME` columns using `INTERVAL`:

```sql
SELECT
    id,
    time1 - time2 AS time_sub,
    time1 + INTERVAL '1' HOUR AS add,
    time2 - INTERVAL '250' MINUTE AS sub
FROM TimeLog;
```

You can also add a `TIME` column to a `DATE` value to get a `TIMESTAMP` result:

```sql
SELECT
    id,
    DATE '2021-01-05' + time2 AS timestamp
FROM TimeLog LIMIT 1;
```

## Handling invalid time values

If you try to insert an invalid time value into a `TIME` column, GlueSQL will return an error:

```sql
INSERT INTO TimeLog VALUES (1, '12345-678', '20:05:01');
```

This will result in an error similar to the following:

```
failed to parse time 12345-678
```

## Conclusion

In GlueSQL, the TIME data type is used to store time values in the format 'HH:MM:SS.SSS'. The provided code snippet demonstrates how to create a table with TIME columns, insert data into it, and perform various queries and operations on the data. GlueSQL supports arithmetic operations on TIME columns using INTERVAL, and you can also add a TIME column to a DATE value to get a TIMESTAMP result. Keep in mind that inserting invalid time values into a TIME column will result in an error.