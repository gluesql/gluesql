# GREATEST

`GREATEST` returns the highest value among the supplied expressions.

## Syntax

```sql
GREATEST(expr1, expr2, ...)
```

## Parameters

- `expr1`, `expr2`, ... – Two or more comparable expressions.

## Examples

```sql
SELECT GREATEST(1, 6, 9, 7, 0, 10) AS result;
```

The query above returns `10`.

## Notes

All arguments must be of comparable types. At least two expressions are required.
