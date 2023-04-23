---
sidebar_position: 3
---

# DELETE

The `DELETE` statement is used to remove records from a table. You can delete a single row, multiple rows, or all rows at once based on specific conditions.

## Basic DELETE Syntax

To delete records from a table, use the following syntax:

```sql
DELETE FROM table_name
WHERE condition;
```

If you want to delete all records from a table, you can omit the `WHERE` clause:

```sql
DELETE FROM table_name;
```

## Examples

Consider the following `Foo` table:

```sql
CREATE TABLE Foo (
    id INTEGER PRIMARY KEY,
    score INTEGER,
    flag BOOLEAN
);
```

With the following records:

```sql
INSERT INTO Foo VALUES
    (1, 100, TRUE),
    (2, 300, FALSE),
    (3, 700, TRUE);
```

### Deleting Records with a WHERE Clause

To delete records that meet a specific condition, use the `WHERE` clause:

```sql
DELETE FROM Foo WHERE flag = FALSE;
```

After executing the above query, the remaining records in the `Foo` table will be:

```
id | score | flag
---+-------+------
1  | 100   | true
3  | 700   | true
```

### Deleting All Records

To delete all records from a table, omit the `WHERE` clause:

```sql
DELETE FROM Foo;
```

After executing the above query, the `Foo` table will be empty:

```
id | score | flag
(no rows)
```