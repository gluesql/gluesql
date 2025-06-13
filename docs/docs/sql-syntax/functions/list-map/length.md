# LENGTH

`LENGTH` returns the number of elements in a list or map, or the number of characters in a string.

## Syntax

```sql
LENGTH(value)
```

## Parameters

- `value` – List, map or string expression.

## Examples

```sql
SELECT LENGTH('Hello.');       -- returns 6
SELECT LENGTH(CAST('[1,2,3]' AS LIST));  -- returns 3
SELECT LENGTH(CAST('{"a":1, "b":5}' AS MAP)); -- returns 2
```

## Notes

If `value` is `NULL` the result is `NULL`.
