# REPLACE

`REPLACE` returns a string with all occurrences of one substring replaced by another substring.

## Syntax

```sql
REPLACE(text, old, new)
```

## Parameters

- `text` - The input string.
- `old` - The substring to replace.
- `new` - The replacement substring.

## Examples

```sql
SELECT REPLACE('Tticky GlueTQL', 'T', 'S') AS name;
```

This returns `Sticky GlueSQL`.

## Notes

`REPLACE` requires exactly three arguments. If any argument is `NULL`, the result is `NULL`.
