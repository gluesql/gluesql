---
title: "Introduction"
sidebar_position: 1
---

# Introduction to SQL Syntax

Welcome to the Introduction page for the SQL Syntax category in GlueSQL! In this section, we'll provide a brief overview of the SQL syntax supported by GlueSQL. You can find more in-depth examples and details by browsing the other pages in this category.

GlueSQL is a SQL database engine written in Rust, designed to be lightweight, fast, and easy to integrate with various data storage systems. It supports a wide range of SQL features, including creating and managing tables, inserting and updating data, and performing various types of queries.

Here's a list of some basic SQL statements you can use with GlueSQL:

## Creating Tables

```sql
CREATE TABLE table_name (
    column_name1 data_type1,
    column_name2 data_type2,
    ...
);
```

## Inserting Data

```sql
INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...);
```

## Selecting Data

```sql
SELECT column1, column2, ... FROM table_name WHERE conditions;
```

## Updating Data

```sql
UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE conditions;
```

## Deleting Data

```sql
DELETE FROM table_name WHERE conditions;
```

For a complete list of supported SQL features, you can visit the GlueSQL GitHub repository's test suite folder, even if you're not familiar with Rust code:
[https://github.com/gluesql/gluesql/tree/main/test-suite/src](https://github.com/gluesql/gluesql/tree/main/test-suite/src)

This folder contains tests for all the functionalities supported by GlueSQL, providing you with an extensive reference for syntax and usage.

Feel free to explore the other pages in this category to dive deeper into GlueSQL's SQL syntax and capabilities!