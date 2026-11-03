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


## CREATE TABLE AS (CTAS)

Use a SELECT or VALUES query to create and populate a table:

```sql
CREATE TABLE table_name AS SELECT * FROM other_table;
CREATE TABLE numbers AS VALUES (1), (2);
```

* `table_name`: The name of the new table to be created.
* `other_table`: The existing table from which the data will be selected.

The column names and types are determined before the source query runs. An empty
result can therefore create a table when its output types are known. If a type
cannot be determined, provide an explicit column definition or a CAST. A column
containing only untyped NULL expressions has no inferred type.

### Copying column definitions

A single `*` or matching `table_alias.*` from one physical table preserves its
column types, nullability, defaults, PRIMARY KEY/UNIQUE constraints, and column
comments. WHERE, ORDER BY, DISTINCT, OFFSET, and LIMIT can select or reorder its
rows without changing this rule. Table aliases can rename the copied columns.

Joins, grouping, aggregates, derived tables, and explicit projection expressions
produce ordinary result columns: their types and names are inferred, all columns
are nullable, and defaults, uniqueness constraints, and comments are not copied.
A whole schemaless map projection produces a schemaless table. A whole SERIES
projection preserves its fixed `N INT NOT NULL` column.

Column-definition copying does not copy foreign keys, secondary indexes, the
storage engine, or the table comment.

### Explicit definitions and types

Explicit definitions take precedence over inferred names and attributes. Source
values are matched by position, and the source must have exactly as many columns
as the target. Missing source columns are not filled with DEFAULT or NULL;
defaults remain available to subsequent INSERT statements.

```sql
CREATE TABLE amounts (amount FLOAT NOT NULL DEFAULT 0)
AS VALUES (1), (2);
```

For VALUES without explicit definitions, each column uses the first expression
whose type can be determined in input order. Untyped NULL is skipped. A schema
type or CAST supplies a type even when its value is NULL. Later literals are
converted using that type; later typed expressions do not widen it.
An expression with an unknown type is not skipped in favor of a later row.

```sql
CREATE TABLE floats AS VALUES (CAST(1 AS FLOAT)), (2); -- FLOAT
CREATE TABLE unknown AS VALUES (NULL);               -- type cannot be inferred
CREATE TABLE typed_null AS VALUES (CAST(NULL AS INT)); -- INT
```

For CASE, COALESCE, IFNULL, and GREATEST, typed alternatives must agree. Untyped
literals are prepared using that shared type, independent of branch order.
Different typed alternatives require an explicit CAST. No general numeric type
promotion is performed.

### Constraints and IF NOT EXISTS

Initial rows must satisfy the target column types, nullability, PRIMARY KEY,
UNIQUE, and foreign key constraints. References to existing tables are supported;
self-referencing foreign keys combined with CTAS are currently unsupported.
Ordinary CREATE TABLE without a source can still define a self-reference.

The source is evaluated and its rows checked before the target schema and data
are written. A storage write failure still follows the storage's transaction
semantics.

`IF NOT EXISTS` still requires a valid source and determinable output columns.
When the target already exists and planning succeeds, the source is not executed
and no rows are inserted.

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
