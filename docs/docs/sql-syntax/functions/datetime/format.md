# FORMAT

The `FORMAT` function in SQL is used to format date, time, and timestamp values into a specified format.

## Syntax

```sql
FORMAT(value, format)
```

- `value`: The date, time, or timestamp value that is to be formatted.
- `format`: The format in which the value is to be displayed. This is a string that contains format specifiers, such as `%Y` for four-digit year, `%m` for two-digit month, and so on.

## Usage

Here are examples of how `FORMAT` can be used to display datetime components in various formats:

1. Formatting a `DATE` value: 
   ```sql
   SELECT FORMAT(DATE '2017-06-15','%Y-%m') AS date;
   ```
   This returns `"2017-06"`.

2. Formatting a `TIMESTAMP` value: 
   ```sql
   SELECT FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S') AS timestamp;
   ```
   This returns `"2015-09-05 23:56:04"`.

3. Formatting a `TIME` value: 
   ```sql
   SELECT FORMAT(TIME '23:56:04','%H:%M') AS time;
   ```
   This returns `"23:56"`.

4. Formatting different components of a `TIMESTAMP` value separately: 
   ```sql
   SELECT 
       FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%Y') AS year,
       FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%m') AS month,
       FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%d') AS day;
   ```
   This returns:

   ```
   year | month | day
   -----+-------+-----
   2015 |    09 |  05
   ```

Please note that the `FORMAT` function only accepts date, time, or timestamp values. If you try to format a value with an incorrect type, you will encounter an error.

## Error Example

```sql
SELECT FORMAT('2015-09-05 23:56:04', '%Y-%m-%d %H') AS timestamp;
```

This will throw an error because the input value is a string, not a date, time, or timestamp value:

```rust
EvaluateError::UnsupportedExprForFormatFunction("2015-09-05 23:56:04".to_owned())
```