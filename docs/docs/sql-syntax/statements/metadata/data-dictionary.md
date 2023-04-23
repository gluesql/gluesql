---
sidebar_position: 2
---

# Data Dictionary

In GlueSQL, there are predefined tables, also known as Data Dictionary tables, which store metadata about the database objects like tables, columns, and indexes. These tables can be queried like any other table in the database, and they provide useful information about the database schema.

The Data Dictionary tables in GlueSQL include:

1. `GLUE_TABLES`
2. `GLUE_TABLE_COLUMNS`
3. `GLUE_INDEXES`

Please note that the columns provided in these tables are the default columns. Storage implementations may provide additional information in these tables.

## GLUE_TABLES

The `GLUE_TABLES` table contains a list of all tables in the database.

Columns:
- `TABLE_NAME`: The name of the table.

## GLUE_TABLE_COLUMNS

The `GLUE_TABLE_COLUMNS` table contains information about the columns in each table.

Columns:
- `TABLE_NAME`: The name of the table that the column belongs to.
- `COLUMN_NAME`: The name of the column.
- `COLUMN_ID`: The column's unique identifier.

## GLUE_INDEXES

The `GLUE_INDEXES` table contains information about the indexes defined in the database.

Columns:
- `TABLE_NAME`: The name of the table that the index belongs to.
- `INDEX_NAME`: The name of the index.
- `ORDER`: The order in which the index is sorted (e.g., "ASC", "DESC", or "BOTH").
- `EXPRESSION`: The expression used for the indexed column (e.g., "id" or "id + 2").
- `UNIQUENESS`: A boolean value indicating whether the index enforces uniqueness.

## Examples

To query the `GLUE_TABLES` table and get a list of all tables in the database:

```sql
SELECT * FROM GLUE_TABLES;
```

To query the `GLUE_TABLE_COLUMNS` table and get information about the columns in each table:

```sql
SELECT * FROM GLUE_TABLE_COLUMNS;
```

To query the `GLUE_INDEXES` table and get information about the indexes defined in the database:

```sql
SELECT * FROM GLUE_INDEXES;
```
