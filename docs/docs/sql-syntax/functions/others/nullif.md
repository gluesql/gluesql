# NULLIF

`NULLIF` compares two expressions. If they are equal, it returns `NULL`; otherwise, it returns the first expression.

## Syntax

```sql
NULLIF(expression1, expression2)
```

## Examples

Return `NULL` when both expressions are equal:

```sql
SELECT NULLIF(0, 0) AS result;
```

Return the first expression when the two expressions are different:

```sql
SELECT NULLIF('hello', 'helle') AS result;
```

This returns `hello`.

## Notes

`NULLIF` requires exactly two arguments.
