# COALESCE

The `COALESCE` function returns the first non-`NULL` value from the list of expressions.

## Syntax

```sql
COALESCE(expr1, expr2, ...)
```

## Parameters

- `expr1`, `expr2`, ... – Expressions evaluated in order. At least one expression must be provided.

## Examples

```sql
CREATE TABLE example (a INT NULL, b INT NULL);
INSERT INTO example VALUES (NULL, 2), (3, NULL), (NULL, NULL);

SELECT COALESCE(a, b, 0) AS result FROM example;
```

This returns `2`, `3` and `0` for the three rows.

## Notes

If all arguments are `NULL`, the result is `NULL`.
