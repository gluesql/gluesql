# VALUES

`VALUES` returns the values of a map as a list.

## Syntax

```sql
VALUES(map)
```

## Parameters

- `map` – Map expression.

## Examples

```sql
SELECT VALUES(CAST('{"id":1, "name":"alice"}' AS MAP));
```

This returns `[1, "alice"]`.

## Notes

A non-map value will cause an error.
