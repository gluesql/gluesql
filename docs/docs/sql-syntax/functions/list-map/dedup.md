# DEDUP

`DEDUP` removes duplicate elements from a list while preserving order.

## Syntax

```sql
DEDUP(list)
```

## Parameters

- `list` – List value to process.

## Examples

```sql
SELECT DEDUP(CAST('[1, 2, 3, 3, 4, 5, 5]' AS LIST));
```

This returns `[1, 2, 3, 4, 5]`.

## Notes

A non-list argument results in an error.
