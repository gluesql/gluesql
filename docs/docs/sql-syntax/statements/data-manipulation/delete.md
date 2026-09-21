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
## Deleting Using Another Table

A `DELETE` may name one additional table in a `USING` clause. The `WHERE` condition decides
which target and source rows pair up and may reference both sides; a bare column name
resolves against the target table first and the source table second.

```sql
DELETE FROM table_name
USING source_table [ AS alias ]
WHERE condition;
```

For example, to remove every `Item` that has a matching `Restock` row:

```sql
DELETE FROM Item USING Restock AS r WHERE id = r.item_id;
```

A target row that matches no source row is kept, and a target row matching several source
rows is removed once. Without a `WHERE` condition every source row matches.

`USING` accepts a single table, and requires both tables to have a declared schema.

## RETURNING

`DELETE` accepts a `RETURNING` clause, which takes the same projection shapes a `SELECT`
does and returns the deleted rows as they looked before the delete:

```sql
DELETE FROM Foo WHERE flag = FALSE RETURNING id, score;
```

A `DELETE` that removes no row returns an empty result that still carries the labels.
`RETURNING` requires a table with a declared schema.

:::note
The SQL parser can read `RETURNING` as an alias for the table right before it, so a
`RETURNING` clause needs either a `WHERE` clause or an explicit alias ahead of it:
`DELETE FROM Foo AS f RETURNING *`.
:::
