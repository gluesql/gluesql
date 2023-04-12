---
sidebar_position: 1
---

# SHOW TABLES

The `SHOW TABLES` statement in GlueSQL is used to display a list of tables available in the database. This statement is useful when you want to inspect the current structure of your database or when you want to manage multiple tables.

## Syntax

```sql
SHOW TABLES;
```

## Example

Consider the following example where we create a few tables and then use the `SHOW TABLES` statement to list them:

```sql
CREATE TABLE Foo (id INTEGER, name TEXT NULL, type TEXT NULL);
CREATE TABLE Zoo (id INTEGER);
CREATE TABLE Bar (id INTEGER, name TEXT NULL);

SHOW TABLES;
```

The output of the `SHOW TABLES` statement will be:

```
Bar
Foo
Zoo
```

The tables are listed in alphabetical order.