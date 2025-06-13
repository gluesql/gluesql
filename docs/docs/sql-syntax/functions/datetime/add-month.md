# ADD_MONTH

`ADD_MONTH` shifts a date by the given number of months.

## Syntax

```sql
ADD_MONTH(date, months)
```

## Parameters

- `date` – A `DATE` value.
- `months` – Integer number of months to add. Negative values subtract months.

## Examples

```sql
SELECT ADD_MONTH('2017-06-15', 1) AS next_month;
SELECT ADD_MONTH('2017-06-15', -1) AS prev_month;
```

These return `2017-07-15` and `2017-05-15` respectively.

## Notes

If the resulting day does not exist in the target month, the last day of that month is used.
