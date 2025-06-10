# IS_EMPTY

`IS_EMPTY` checks whether a list or map contains no elements.

## Syntax

```sql
IS_EMPTY(value)
```

## Parameters

- `value` – List or map expression.

## Examples

```sql
SELECT IS_EMPTY(CAST('[]' AS LIST));    -- true
SELECT IS_EMPTY(CAST('{"a":1}' AS MAP)); -- false
```

## Notes

Using other data types results in an error.
