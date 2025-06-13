# ENTRIES

`ENTRIES` converts a map into a list of key–value pairs.

## Syntax

```sql
ENTRIES(map)
```

## Parameters

- `map` – Map expression to convert.

## Examples

```sql
SELECT ENTRIES(CAST('{"name":"GlueSQL"}' AS MAP));
```

This returns `[["name", "GlueSQL"]]`.

## Notes

`ENTRIES` requires a map argument.
