# LAST_DAY

The `LAST_DAY` function returns the last day of the month of a given date or timestamp.

## Syntax

```sql
LAST_DAY(value)
```

## Parameters

- `value` – A `DATE` or `TIMESTAMP` expression.

## Examples

```sql
SELECT LAST_DAY('2017-12-15');
```

This returns `2017-12-31`.

## Notes

`LAST_DAY` accepts only date or timestamp values; other types produce an error.
