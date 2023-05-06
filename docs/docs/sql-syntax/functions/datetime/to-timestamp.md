# TO_TIMESTAMP

The `TO_TIMESTAMP` function in SQL is used to convert a string into a TIMESTAMP. This function takes two arguments, the string to be converted and a format string that specifies the format of the input string.

## Syntax

```sql
TO_TIMESTAMP(string, format)
```

## Examples

### Converting a string to a TIMESTAMP

```sql
VALUES(TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S'));
```

In this example, the string '2015-09-05 23:56:04' is converted into a TIMESTAMP using the format '%Y-%m-%d %H:%M:%S', where %Y is the four-digit year, %m is the two-digit month, %d is the two-digit day, %H is the two-digit hour, %M is the two-digit minute, and %S is the two-digit second.

### Selecting a converted string to a TIMESTAMP

```sql
SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S') AS timestamp;
```

In this example, the string '2015-09-05 23:56:04' is converted into a TIMESTAMP using the format '%Y-%m-%d %H:%M:%S' and selected as 'timestamp'.

## Error Handling

The `TO_TIMESTAMP` function requires a string value as its first argument. If a non-string value is provided, it will return an error.

```sql
SELECT TO_TIMESTAMP(TIMESTAMP '2015-09-05 23:56:04','%Y-%m-%d') AS timestamp;
```

In this case, the TIMESTAMP '2015-09-05 23:56:04' is not a string and will cause an error.

Additionally, if the format string does not match the format of the input string, an error will also be returned. For example:

```sql
SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M') AS timestamp;
```

In this case, the format string '%Y-%m-%d %H:%M' does not match the input string '2015-09-05 23:56:04', so an error will be returned.