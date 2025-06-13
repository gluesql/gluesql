# KEYS

`KEYS` returns the keys of a map as a list.

## Syntax

```sql
KEYS(map)
```

## Parameters

- `map` – Map expression.

## Examples

```sql
SELECT KEYS(CAST('{"id":1, "name":"alice"}' AS MAP));
```

This returns `["id", "name"]`.

## Notes

A non-map value will cause an error.
