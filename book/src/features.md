# Implemented SQL Features

GlueSQL currently supports limited queries, it's in very early stage.

- `CREATE` with 4 types: `INTEGER`, `FLOAT`, `BOOLEAN`, `TEXT` with an optional `NULL` attribute.
- `ALTER TABLE` with 4 operations: `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN` and `RENAME TO`.
- `INSERT`, `UPDATE`, `DELETE`, `SELECT`, `DROP TABLE`
- `GROUP BY`, `HAVING`
- Nested select, join, aggregations ...

You can see current query supports in [src/tests/\*](https://github.com/gluesql/gluesql/tree/main/src/tests).
