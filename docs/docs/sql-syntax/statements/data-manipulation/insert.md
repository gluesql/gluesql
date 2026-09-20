---
sidebar_position: 1
---

# INSERT

The `INSERT` statement is used to insert new records into a table. You can insert a single row or multiple rows at once, and you can also use the `NULL`, `NOT NULL`, and `DEFAULT` constraints to define how values are inserted.

## Basic INSERT Syntax

```sql
INSERT INTO table_name (column1, column2, column3, ...)
VALUES
    (value1, value2, value3, ...),
    (value4, value5, value6, ...),
    ...
;
```

## Handling NULL, NOT NULL, and DEFAULT Constraints

When inserting data into a table, the database handles `NULL`, `NOT NULL`, and `DEFAULT` constraints as follows:

- **NULL**: If a column is defined with the `NULL` constraint (or no constraint is provided), you can insert a `NULL` value or omit the column in the `INSERT` statement. The database will store a `NULL` value for the omitted column.

- **NOT NULL**: If a column is defined with the `NOT NULL` constraint, you must provide a value for the column in the `INSERT` statement. If you try to insert a `NULL` value or omit the column, the database will return an error.

- **DEFAULT**: If a column is defined with a `DEFAULT` value, you can omit the column in the `INSERT` statement. The database will automatically use the default value for the omitted column.

## Examples

Consider the following `Test` table:

```sql
CREATE TABLE Test (
    id INTEGER DEFAULT 1,
    num INTEGER NULL,
    name TEXT NOT NULL
);
```

### Basic INSERT

To insert a single row:

```sql
INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hi boo');
```

### Inserting Multiple Rows

To insert multiple rows at once:

```sql
INSERT INTO Test (id, num, name)
VALUES
    (3, 9, 'Kitty!'),
    (2, 7, 'Monsters');
```

### Inserting with Omitted Columns

If you want to insert a row without specifying a value for a column with a `DEFAULT` constraint, you can simply omit the column:

```sql
INSERT INTO Test (num, name) VALUES (28, 'Wazowski');
```

For columns with `NULL` constraint, you can either omit the column or explicitly insert a `NULL` value:

```sql
INSERT INTO Test (name) VALUES ('The end');
```

### Handling NOT NULL Constraint

If you try to insert a row without specifying a value for a column with the `NOT NULL` constraint, the database will return an error:

```sql
INSERT INTO Test (id, num) VALUES (1, 10);
-- Error: LackOfRequiredColumn("name")
```
## ON CONFLICT

A row whose `PRIMARY KEY` or `UNIQUE` value is already in the table would normally end
the statement with a duplicate-entry error. `ON CONFLICT` says what to do with it
instead: leave the table alone, or update the row that is already there.

```sql
INSERT INTO table_name (column1, column2, ...)
VALUES (value1, value2, ...)
ON CONFLICT [(column)] DO NOTHING;

INSERT INTO table_name (column1, column2, ...)
VALUES (value1, value2, ...)
ON CONFLICT (column) DO UPDATE SET column2 = ... [WHERE condition];
```

The column in brackets is the **conflict target**: the constraint being watched. It must
be a `PRIMARY KEY` or `UNIQUE` column of the table, and one column, since a constraint
here covers a single column. `DO NOTHING` may leave it out, which watches every unique
column of the table; `DO UPDATE` requires it.

### DO NOTHING

```sql
CREATE TABLE Stock (id INTEGER PRIMARY KEY, sku TEXT UNIQUE, quantity INTEGER);
INSERT INTO Stock VALUES (1, 'apple', 10);

INSERT INTO Stock VALUES (1, 'apple', 99) ON CONFLICT DO NOTHING;
-- 0 rows inserted; the stored row keeps its quantity of 10
```

### DO UPDATE and `excluded`

`DO UPDATE` writes the row already in the table. Its assignments see that row by column
name, and the row the insert proposed under the alias `excluded` — the name PostgreSQL
uses, and `EXCLUDED` works as well:

```sql
INSERT INTO Stock VALUES (1, 'apple', 5)
ON CONFLICT (id) DO UPDATE SET quantity = quantity + excluded.quantity;
-- the stored row now has a quantity of 15
```

An upsert, in other words: insert when the row is new, update when it is not. The count
the statement reports is the rows inserted plus the rows updated.

`WHERE` after the assignments makes the update conditional; when it is false the row is
left as it is and nothing is inserted:

```sql
INSERT INTO Stock VALUES (1, 'apple', 100)
ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity WHERE excluded.quantity < 50;
```

### What it does not do

- `ON CONFLICT ON CONSTRAINT <name>` — constraints are not named here, so name the
  column instead.
- A conflict target of several columns, which would be a composite unique constraint.
- Conflicts on a constraint the target does not name are still errors, as in PostgreSQL:
  `ON CONFLICT (id) DO NOTHING` does not cover a duplicate `sku`.
- `DO UPDATE` writes one stored row once. Two rows in the same statement conflicting
  with the same stored row is an error, again as in PostgreSQL.
- A `NULL` conflicts with nothing, because a unique constraint does not constrain nulls.
