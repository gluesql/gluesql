---
sidebar_position: 2
---

# UPDATE

The `UPDATE` statement is used to modify existing records in a table. You can update one or more columns with new values, or even use subqueries to update values based on other tables.

## Basic UPDATE Syntax

```sql
UPDATE table_name
SET column1 = value1, column2 = value2, ...
WHERE condition;
```

## Examples

### Updating a Single Column

Consider the following `TableA`:

| id | num | num2 | name |
|----|-----|------|------|
| 1  | 2   | 4    | Hello|
| 1  | 9   | 5    | World|
| 3  | 4   | 7    | Great|
| 4  | 7   | 10   | Job  |

To update the `id` column for all rows in `TableA`, you can use the following query:

```sql
UPDATE TableA SET id = 2;
```

The resulting `TableA` would look like this:

| id | num | num2 | name |
|----|-----|------|------|
| 2  | 2   | 4    | Hello|
| 2  | 9   | 5    | World|
| 2  | 4   | 7    | Great|
| 2  | 7   | 10   | Job  |

### Updating with a Condition

If you want to update only specific rows that meet a certain condition, you can use the `WHERE` clause. For example, to update the `id` column only for the row with `num = 9`:

```sql
UPDATE TableA SET id = 4 WHERE num = 9;
```

### Updating with a Subquery

You can also use a subquery in the `UPDATE` statement to update a column based on other table's values. For example, to update the `num2` column in `TableA` with the `rank` column value from `TableB` where the `num` column values match, and the `num = 7`:

```sql
UPDATE TableA SET num2 = (SELECT rank FROM TableB WHERE num = TableA.num) WHERE num = 7;
```

### Updating Based on the Result of Another Query

You can update a column based on the result of another query. For example, to update the `num2` column in `TableA` with the `rank` column value from `TableB` where the `num` column values match, and the `num` is the minimum `num` in `TableA`:

```sql
UPDATE TableA SET num2 = (SELECT rank FROM TableB WHERE num = TableA.num) WHERE num = (SELECT MIN(num) FROM TableA);
```

## Not Supported Features

- Using `JOIN` in an `UPDATE` statement is not supported.
- Updating a table using compound identifiers (e.g., `ErrTestTable.id = 1`) is not supported.
- Updating a non-existent table will result in a `TableNotFound` error.
- Updating a non-existent column will result in a `ColumnNotFound` error.