# TO_TIME

The `TO_TIME` function in SQL is used to convert a string into a TIME. This function takes two arguments, the string to be converted and a format string that specifies the format of the input string.

## Syntax

```sql
TO_TIME(string, format)
```

## Examples

### Converting a string to a TIME

```sql
VALUES(TO_TIME('23:56:04', '%H:%M:%S'));
```

In this example, the string '23:56:04' is converted into a TIME using the format '%H:%M:%S', where %H is the two-digit hour, %M is the two-digit minute, and %S is the two-digit second.

### Selecting a converted string to a TIME

```sql
SELECT TO_TIME('23:56:04','%H:%M:%S') AS time;
```

In this example, the string '23:56:04' is converted into a TIME using the format '%H:%M:%S' and selected as 'time'.

## Error Handling

The `TO_TIME` function requires a string value as its first argument. If a non-string value is provided, it will return an error.

```sql
SELECT TO_TIME(TIME '23:56:04','%H:%M:%S') AS time;
```

In this case, the TIME '23:56:04' is not a string and will cause an error.

Additionally, if the format string does not match the format of the input string, an error will also be returned. For example:

```sql
SELECT TO_TIME('23:56', '%H:%M:%S') AS time;
```

In this case, the format string '%H:%M:%S' does not match the input string '23:56', so an error will be returned.