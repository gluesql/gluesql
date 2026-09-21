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

An `INSERT` may end with an `ON CONFLICT` clause that says what to do when a row would
duplicate a value in a `PRIMARY KEY` or `UNIQUE` column.

```sql
INSERT INTO table_name (column1, column2, ...)
VALUES (value1, value2, ...)
ON CONFLICT [ ( conflict_column ) ] DO NOTHING;

INSERT INTO table_name (column1, column2, ...)
VALUES (value1, value2, ...)
ON CONFLICT ( conflict_column ) DO UPDATE SET column1 = expression, ...
[ WHERE condition ];
```

The optional conflict target restricts the check to the named column, which must be a
`PRIMARY KEY` or `UNIQUE` column. `DO NOTHING` without a target checks every such column.
`DO UPDATE` always requires a target.

### DO NOTHING

Conflicting rows are skipped and the remaining rows are inserted:

```sql
CREATE TABLE Item (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER);
INSERT INTO Item VALUES (1, 'pen', 3), (2, 'ink', 5);

INSERT INTO Item VALUES (1, 'dup', 9), (3, 'pad', 7) ON CONFLICT DO NOTHING;
-- 1 row inserted, the row with id 1 is skipped
```

### DO UPDATE

The stored row receives the assignments. Their expressions read the stored row's columns
directly, and the row that failed to insert through the `excluded` alias:

```sql
INSERT INTO Item VALUES (2, 'ink-v2', 10)
ON CONFLICT (id) DO UPDATE SET qty = qty + excluded.qty, name = excluded.name;
```

An optional `WHERE` condition, which may also reference `excluded`, decides whether the
update happens at all. A row whose condition does not hold is left untouched:

```sql
INSERT INTO Item VALUES (1, 'pen', 5)
ON CONFLICT (id) DO UPDATE SET qty = excluded.qty WHERE excluded.qty > qty;
```

Rows are processed in statement order, so a row inserted earlier in the same statement can
be the one a later row conflicts with. `NULL` never conflicts with anything. The primary
key may not be assigned in `DO UPDATE`, an update may not create a duplicate in another
unique column, and a conflict on a unique column outside the conflict target still fails
the statement the way a plain `INSERT` does.

`ON CONFLICT` requires a table with a declared schema.

## RETURNING

`INSERT`, `UPDATE` and `DELETE` accept a `RETURNING` clause that takes the same projection
shapes a `SELECT` does, and turns the statement's result into rows instead of a count:

```sql
INSERT INTO Item VALUES (4, 'clip', 1) RETURNING *;
INSERT INTO Item VALUES (5, 'tape', 7) RETURNING id, qty * 2 AS double;
```

An `INSERT` returns the stored rows in statement order, including rows an `ON CONFLICT`
update touched with their final values, and excluding the ones it skipped. A statement
that affects no rows returns an empty result that still carries the labels.

`RETURNING` projections are evaluated before the statement's rows reach the storage, so a projection that fails to evaluate leaves the table unchanged, and a subquery inside `RETURNING` reads the table as it stood before the statement.

`RETURNING` requires a table with a declared schema.
