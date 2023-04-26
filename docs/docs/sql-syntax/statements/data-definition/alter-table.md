---
sidebar_position: 5
---

# ALTER TABLE

The `ALTER TABLE` statement is an SQL command used to modify the structure of an existing table in a database. This operation is useful when you need to add, remove, or modify columns or constraints in a table. In this document, we'll explain the syntax and usage of the `ALTER TABLE` statement, including the `RENAME`, `ADD COLUMN`, and `DROP COLUMN` clauses.

## Syntax

The basic syntax for the `ALTER TABLE` statement is as follows:

```sql
ALTER TABLE table_name action;
```

- `table_name`: The name of the table you want to alter.
- `action`: The action you want to perform on the table, such as renaming the table, adding a new column, or dropping an existing column.

### RENAME

To rename a table or a column, use the following syntax:

```sql
ALTER TABLE table_name RENAME [TO new_table_name | COLUMN column_name TO new_column_name];
```

### ADD COLUMN

To add a new column to a table, use the following syntax:

```sql
ALTER TABLE table_name ADD COLUMN column_name datatype [DEFAULT default_value] [NOT NULL] [UNIQUE];
```

### DROP COLUMN

To drop an existing column from a table, use the following syntax:

```sql
ALTER TABLE table_name DROP COLUMN column_name;
```

## Examples

1. Renaming a table:

```sql
ALTER TABLE employees RENAME TO staff;
```

This command will rename the `employees` table to `staff`.

2. Renaming a column:

```sql
ALTER TABLE employees RENAME COLUMN first_name TO given_name;
```

This command will rename the `first_name` column to `given_name` in the `employees` table.

3. Adding a new column:

```sql
ALTER TABLE employees ADD COLUMN department TEXT;
```

This command will add a new `department` column with the `TEXT` datatype to the `employees` table.

4. Adding a new column with a default value:

```sql
ALTER TABLE employees ADD COLUMN active BOOLEAN DEFAULT true;
```

This command will add a new `active` column with the `BOOLEAN` datatype and a default value of `true` to the `employees` table.

5. Dropping a column:

```sql
ALTER TABLE employees DROP COLUMN department;
```

This command will remove the `department` column from the `employees` table.

## Summary

The `ALTER TABLE` statement is an essential SQL command that allows you to modify the structure of an existing table in a database. It supports renaming tables and columns, adding new columns with optional default values and constraints, and dropping existing columns. By understanding the `ALTER TABLE` syntax, you can efficiently manage your database schema and make necessary changes to your tables as your data requirements evolve.
