# TAKE

`TAKE` returns the first N elements from a list.

## Syntax

```sql
TAKE(list, count)
```

## Parameters

- `list` – List value.
- `count` – Number of elements to take. Must be a non‑negative integer.

## Examples

```sql
SELECT TAKE(CAST('[1,2,3,4,5]' AS LIST), 3);
```

This returns `[1, 2, 3]`.
