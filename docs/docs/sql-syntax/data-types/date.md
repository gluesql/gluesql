---
sidebar_position: 6
---

# DATE

In GlueSQL, the `DATE` data type is used to store date values in the format 'YYYY-MM-DD'. Note that GlueSQL currently does not support timezones.

## Creating a table with DATE columns

To create a table with columns of type `DATE`, use the `CREATE TABLE` statement:

```sql
CREATE TABLE DateLog (
    id INTEGER,
    date1 DATE,
    date2 DATE
);
```

## Inserting data into a table with DATE columns

To insert data into a table with `DATE` columns, use the `INSERT INTO` statement:

```sql
INSERT INTO DateLog VALUES
    (1, '2020-06-11', '2021-03-01'),
    (2, '2020-09-30', '1989-01-01'),
    (3, '2021-05-01', '2021-05-01');
```

## Querying data from a table with DATE columns

To query data from a table with `DATE` columns, use the `SELECT` statement:

```sql
SELECT id, date1, date2 FROM DateLog;
```

## Filtering data using DATE columns

You can use various comparison operators like `>`, `<`, `<=`, `>=`, and `=` to filter data based on `DATE` columns:

```sql
SELECT * FROM DateLog WHERE date1 > date2;

SELECT * FROM DateLog WHERE date1 <= date2;

SELECT * FROM DateLog WHERE date1 = DATE '2020-06-11';

SELECT * FROM DateLog WHERE date2 < '2000-01-01';

SELECT * FROM DateLog WHERE '1999-01-03' < DATE '2000-01-01';
```

## Performing date arithmetic

You can perform arithmetic operations on `DATE` columns using `INTERVAL` and various date arithmetic operators:

```sql
SELECT
    id,
    date1 - date2 AS date_sub,
    date1 - INTERVAL '1' DAY AS sub,
    date2 + INTERVAL '1' MONTH AS add
FROM DateLog;
```

## Handling invalid date values

If you try to insert an invalid date value into a `DATE` column, GlueSQL will return an error:

```sql
INSERT INTO DateLog VALUES (1, '12345-678', '2021-05-01');
```

This will result in an error similar to the following:

```
failed to parse date 12345-678
```

## Conclusion

In summary, the `DATE` data type in GlueSQL allows you to store and manipulate date values in your database. You can create tables with `DATE` columns, insert and query data, filter data based on date comparisons, and perform date arithmetic using various operators and intervals. Always remember to use valid date formats when inserting data into `DATE` columns to avoid errors.