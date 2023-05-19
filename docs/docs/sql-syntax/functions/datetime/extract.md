# EXTRACT

The `EXTRACT` function in SQL is used to retrieve a specific datetime field from a date, time, or interval. 

## Syntax

```sql
EXTRACT(field FROM source)
```

- `field`: The datetime field to extract. Valid fields include `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`.
- `source`: The date, time, or interval value from which the datetime field is to be extracted.

## Usage

Here are examples of how `EXTRACT` can be used to pull specific datetime components from various types of datetime and interval data. 

1. Extracting the `HOUR` from a `TIMESTAMP`: 
   ```sql
   SELECT EXTRACT(HOUR FROM TIMESTAMP '2016-12-31 13:30:15') as extract;
   ```
   This returns `13`.

2. Extracting the `YEAR` from a `TIMESTAMP`: 
   ```sql
   SELECT EXTRACT(YEAR FROM TIMESTAMP '2016-12-31 13:30:15') as extract;
   ```
   This returns `2016`.

3. Extracting the `SECOND` from a `TIME` value: 
   ```sql
   SELECT EXTRACT(SECOND FROM TIME '17:12:28') as extract;
   ```
   This returns `28`.

4. Extracting the `DAY` from a `DATE` value: 
   ```sql
   SELECT EXTRACT(DAY FROM DATE '2021-10-06') as extract;
   ```
   This returns `6`.

5. Extracting from `INTERVAL` data: 
   ```sql
   SELECT EXTRACT(YEAR FROM INTERVAL '3' YEAR) as extract;
   SELECT EXTRACT(MINUTE FROM INTERVAL '7' MINUTE) as extract;
   ```
   These return `3` and `7`, respectively.

Note that the `EXTRACT` function expects the `source` to be of a compatible datetime or interval type. Using a value of an incompatible type, such as a number or a string that cannot be interpreted as a datetime, will result in an error.