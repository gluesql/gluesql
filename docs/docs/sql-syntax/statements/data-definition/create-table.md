---
sidebar_position: 1
---

# CREATE TABLE

The `CREATE TABLE` statement is a fundamental SQL command used to create a new table in a database. Tables are the primary structure in databases, as they hold the data organized in rows and columns. In this document, we'll explain the syntax and usage of the `CREATE TABLE` statement, including the `IF NOT EXISTS` clause.

## Syntax

The basic syntax for the `CREATE TABLE` statement is as follows:

```sql
CREATE TABLE [IF NOT EXISTS] table_name (
    column1 datatype constraint,
    column2 datatype constraint,
    column3 datatype constraint,
    ...
);
```

- `IF NOT EXISTS`: This optional clause allows you to check if a table with the same name already exists in the database. If it exists, the command does nothing; otherwise, it creates a new table.
- `table_name`: The name of the table you want to create.
- `column`: Each column in the table is defined by its name, datatype, and optional constraints.
- `datatype`: The type of data that the column will store (e.g., INTEGER, TEXT, DATE, etc.).
- `constraint`: Optional constraints to enforce rules on the data stored in the column (e.g., PRIMARY KEY, NOT NULL, UNIQUE, etc.).


## CREATE TABLE AS SELECT (CTAS)
You can also create a new table based on the result of a SELECT statement using the CTAS syntax:

```sql
CREATE TABLE table_name AS SELECT * FROM other_table;
```

* `table_name`: The name of the new table to be created.
* `other_table`: The existing table from which the data will be selected.

This command creates a new table with the same column structure as the source table and populates it with the data returned by the SELECT statement. The SELECT statement in this example uses the wildcard *, meaning that all columns from the source table will be included in the new table.

## Example

Let's create a simple table called `employees` with the following columns:

- `id`: A unique identifier for each employee (integer, primary key).
- `first_name`: The employee's first name (text, not null).
- `last_name`: The employee's last name (text, not null).
- `email`: The employee's email address (text, unique).
- `hire_date`: The date the employee was hired (date).

The SQL statement to create this table, using the `IF NOT EXISTS` clause, would look like this:

```sql
CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE,
    hire_date DATE
);
```

## Example with CTAS
Assuming there is an existing table named employees_backup, you can create a new table called employees_copy with the same structure and data using CTAS:

```sql
CREATE TABLE employees_copy AS SELECT * FROM employees_backup;
```

## Constraints

Constraints are rules that you can apply to columns in a table to control the data being stored. Some common constraints are:

- `PRIMARY KEY`: Uniquely identifies each row in the table.
- `NOT NULL`: Ensures the column cannot store a NULL value.
- `UNIQUE`: Ensures all values in the column are unique.
- `DEFAULT`: Sets a default value for the column when no value is specified.

## Summary

The `CREATE TABLE` statement is an essential SQL command that allows you to create tables in a database. It requires a table name and one or more column definitions with their respective datatypes and optional constraints. The `IF NOT EXISTS` clause can be used to prevent creating duplicate tables. By understanding the `CREATE TABLE` syntax, you can define the structure of your tables and ensure the data stored in them is accurate and reliable.
