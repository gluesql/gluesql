---
sidebar_position: 3
---

# CREATE INDEX

`CREATE INDEX` statement is used to create an index on one or more columns of a table. Indexes can improve query performance by allowing the database to quickly locate rows with specific column values. They can also be used with the `ORDER BY` clause to improve sorting performance. An index can be thought of as a data structure that maps the values of a specific column or columns to the corresponding rows in a table. This mapping allows the database to perform lookups and sorting operations more efficiently, as it does not have to scan the entire table.

## Syntax

```sql
CREATE INDEX index_name ON table_name (column_name_expression);
```

- `index_name`: The name of the index. It is recommended to use a descriptive name that indicates the purpose of the index, such as the column(s) it is based on.
- `table_name`: The name of the table on which the index is to be created.
- `column_name_expression`: The column name or expression on which the index is based. Only single column indexes are supported. If a column expression is used, it should be a simple arithmetic operation, such as addition or multiplication.

## Example

Consider the following table called `Students`:

```sql
CREATE TABLE Students (
    id INTEGER,
    age INTEGER,
    name TEXT
);
```

You can create an index on the `id` column with the following statement:

```sql
CREATE INDEX idx_id ON Students (id);
```

You can also create an index on a column expression, such as `age * 2`:

```sql
CREATE INDEX idx_age ON Students (age * 2);
```

Note that composite indexes (indexes on multiple columns) are not supported. These types of indexes can provide additional performance benefits in certain situations, but they also come with added complexity and increased storage requirements.

## Using Index with ORDER BY

Indexes can improve the performance of the `ORDER BY` clause. When an index exists on the column specified in the `ORDER BY` clause, the database can use the index to sort the data more efficiently. This is because the index already provides a sorted view of the data, allowing the database to avoid the cost of sorting the entire table during query execution.

For example, if you have the following query:

```sql
SELECT * FROM Students ORDER BY id ASC;
```

The database can use the `idx_id` index created earlier to sort the data more quickly than without an index. Keep in mind that the performance gains from using an index with the `ORDER BY` clause will depend on the size of the table, the distribution of the data, and the specific database implementation.

