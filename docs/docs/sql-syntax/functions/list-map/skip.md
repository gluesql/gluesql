# SKIP

`SKIP` drops the first N elements from a list and returns the remaining values.

## Syntax

```sql
SKIP(list, count)
```

## Parameters

- `list` – List value.
- `count` – Number of elements to drop. Must be a non‑negative integer.

## Examples

```sql
SELECT SKIP(CAST('[1,2,3,4,5]' AS LIST), 2);
```

This returns `[3, 4, 5]`.
