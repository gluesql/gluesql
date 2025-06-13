# MD5

`MD5` calculates the MD5 hash of a string.

## Syntax

```sql
MD5(text)
```

## Parameters

- `text` – The string to hash.

## Examples

```sql
SELECT MD5('GlueSQL');
```

This returns `4274ecec96f3ee59b51b168dc6137231`.

## Notes

`MD5` requires exactly one argument.
