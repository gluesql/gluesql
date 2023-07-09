# TO_DATE

The `TO_DATE` function in SQL is used to convert a string into a DATE. This function takes two arguments, the string to be converted and a format string that specifies the format of the input string.

## Syntax

```sql
TO_DATE(string, format)
```

## Examples

### Converting a string to a DATE

```sql
VALUES(TO_DATE('2017-06-15', '%Y-%m-%d'));
```

In this example, the string '2017-06-15' is converted into a DATE using the format '%Y-%m-%d', where %Y is the four-digit year, %m is the two-digit month, and %d is the two-digit day.

### Converting a string to a DATE with a different format

```sql
SELECT TO_DATE('2017-jun-15','%Y-%b-%d') AS date;
```

In this example, the string '2017-jun-15' is converted into a DATE using the format '%Y-%b-%d', where %Y is the four-digit year, %b is the abbreviated month name, and %d is the two-digit day.

## Error Handling

The `TO_DATE` function requires a string value as its first argument. If a non-string value is provided, it will return an error.

```sql
SELECT TO_DATE(DATE '2017-06-15','%Y-%m-%d') AS date;
```

In this case, the DATE '2017-06-15' is not a string and will cause an error.

Additionally, if the format string does not match the format of the input string, an error will also be returned. For example:

```sql
SELECT TO_DATE('2015-09-05', '%Y-%m') AS date;
```

In this case, the format string '%Y-%m' does not match the input string '2015-09-05', so an error will be returned.