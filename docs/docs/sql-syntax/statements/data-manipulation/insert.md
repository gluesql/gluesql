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