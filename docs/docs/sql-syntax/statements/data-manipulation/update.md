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

## Updating from Another Table

An `UPDATE` may name one additional table in a `FROM` clause. The `WHERE` condition decides
which target and source rows pair up and may reference both sides; a bare column name
resolves against the target table first and the source table second.

```sql
UPDATE table_name
SET column1 = expression, ...
FROM source_table [ AS alias ]
WHERE condition;
```

For example, with a `Restock` table holding `(item_id, amount)` pairs:

```sql
UPDATE Item SET qty = qty + r.amount FROM Restock AS r WHERE id = r.item_id;
```

A target row that matches no source row is left untouched, and a target row matching more
than one source row fails the statement. Without a `WHERE` condition every source row
matches.

`FROM` requires both tables to have a declared schema.

## RETURNING

`UPDATE` accepts a `RETURNING` clause, which takes the same projection shapes a `SELECT`
does and returns the updated rows as they look after the update:

```sql
UPDATE Item SET qty = qty * 10 WHERE id = 1 RETURNING id, qty AS scaled;
```

With a `FROM` clause the projection may also reference the matched source row's columns.
A star still expands to the target table's columns only:

```sql
UPDATE Item SET qty = qty + r.amount FROM Restock AS r WHERE id = r.item_id
RETURNING id, qty, r.amount AS applied;
```

An `UPDATE` that matches no row returns an empty result that still carries the labels.
`RETURNING` requires a table with a declared schema.

:::note
The SQL parser can read `RETURNING` as an alias for the table right before it, so a
`RETURNING` clause needs either a `WHERE` clause or an explicit alias ahead of it:
`UPDATE Item SET qty = 0 FROM Restock AS r RETURNING *`.
:::

## Not Supported Features

- Using `JOIN` in an `UPDATE` statement is not supported.
- More than one table in the `FROM` clause is not supported.
- Updating a table using compound identifiers (e.g., `ErrTestTable.id = 1`) is not supported.
- Updating a non-existent table will result in a `TableNotFound` error.
- Updating a non-existent column will result in a `ColumnNotFound` error.