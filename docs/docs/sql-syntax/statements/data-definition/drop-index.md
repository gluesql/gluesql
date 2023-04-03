---
sidebar_position: 4
---

# DROP INDEX

`DROP INDEX` statement is used to remove an existing index from a table. This can be useful when an index is no longer needed, or if you want to free up storage space and reduce maintenance overhead associated with maintaining the index.

## Syntax

```sql
DROP INDEX table_name.index_name;
```

- `table_name`: The name of the table containing the index to be dropped.
- `index_name`: The name of the index to be dropped.

Note that only one index can be dropped at a time using the `DROP INDEX` statement. If you want to drop multiple indexes, you need to execute multiple `DROP INDEX` statements.

## Example

Consider the following table called `Students` with an index on the `id` column:

```sql
CREATE TABLE Students (
    id INTEGER,
    age INTEGER,
    name TEXT
);

CREATE INDEX idx_id ON Students (id);
```

You can drop the `idx_id` index with the following statement:

```sql
DROP INDEX Students.idx_id;
```

If you attempt to drop multiple indexes in a single statement, an error will be raised:

```sql
DROP INDEX Students.idx_id, Students.idx_age;
```

This will result in an error, as only one index can be dropped at a time. To drop both indexes, execute two separate `DROP INDEX` statements:

```sql
DROP INDEX Students.idx_id;
DROP INDEX Students.idx_age;
```

