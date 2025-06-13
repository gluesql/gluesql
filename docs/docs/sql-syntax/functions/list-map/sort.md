# SORT

`SORT` orders the elements of a list.

## Syntax

```sql
SORT(list [, 'ASC' | 'DESC'])
```

## Parameters

- `list` – List to sort.
- Optional sort order `'ASC'` (default) or `'DESC'`.

## Examples

```sql
SELECT SORT(CAST('[3,1,4,2]' AS LIST));         -- [1,2,3,4]
SELECT SORT(CAST('[3,1,4,2]' AS LIST), 'DESC'); -- [4,3,2,1]
```

## Notes

Non-comparable values or invalid order strings will produce an error.
