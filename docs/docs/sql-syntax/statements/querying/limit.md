---
sidebar_position: 3
---

# LIMIT & OFFSET

`LIMIT` and `OFFSET` are SQL clauses that allow you to control the number of rows returned by a `SELECT` statement. They are particularly useful when you need to paginate or retrieve a specific portion of the result set.

## LIMIT

The `LIMIT` clause restricts the number of rows returned by a query. The syntax for using `LIMIT` is:

```sql
SELECT columns FROM table_name
LIMIT number_of_rows;
```

## OFFSET

The `OFFSET` clause is used in combination with `LIMIT` to skip a specific number of rows before starting to return the rows. The syntax for using `OFFSET` is:

```sql
SELECT columns FROM table_name
LIMIT number_of_rows
OFFSET number_of_rows_to_skip;
```

You can also use `OFFSET` without `LIMIT`:

```sql
SELECT columns FROM table_name
OFFSET number_of_rows_to_skip;
```

## Examples

Consider the following `Test` table:

```sql
CREATE TABLE Test (
    id INTEGER
);
```

With the following records:

```sql
INSERT INTO Test VALUES (1), (2), (3), (4), (5), (6), (7), (8);
```

### Using LIMIT

Retrieve the first 3 rows from the `Test` table:

```sql
SELECT * FROM Test LIMIT 3;
```

Result:

```
id
---
1
2
3
```

### Using LIMIT and OFFSET

Retrieve the next 4 rows after the first 3 rows from the `Test` table:

```sql
SELECT * FROM Test LIMIT 4 OFFSET 3;
```

Result:

```
id
---
4
5
6
7
```

### Using OFFSET without LIMIT

Retrieve all rows after the first 2 rows from the `Test` table:

```sql
SELECT * FROM Test OFFSET 2;
```

Result:

```
id
---
3
4
5
6
7
8
```