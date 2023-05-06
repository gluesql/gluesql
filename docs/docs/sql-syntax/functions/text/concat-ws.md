# CONCAT_WS

The CONCAT_WS function in SQL concatenates two or more strings into one string with a separator. 

## Syntax

The syntax for the CONCAT_WS function in SQL is:

```sql
CONCAT_WS ( separator, string1, string2, ..., stringN )
```

## Parameters

- `separator`: This is the string that will be placed between each string to be concatenated.
- `string1`, `string2`, ..., `stringN`: These are the strings that you wish to concatenate together. 

## Examples

Let's consider a few examples to understand how to use the CONCAT_WS function.

To concatenate strings with a comma separator:

```sql
VALUES(CONCAT_WS(',', 'AB', 'CD', 'EF'));
```

This will return `'AB,CD,EF'`.

You can also concatenate more than two strings:

```sql
SELECT CONCAT_WS('/', 'ab', 'cd', 'ef') AS myconcat;
```

This will return `'ab/cd/ef'`.

The CONCAT_WS function will skip any NULL values:

```sql
SELECT CONCAT_WS('', 'ab', 'cd', NULL, 'ef') AS myconcat;
```

This will return `'abcdef'`.

The CONCAT_WS function can also take non-string arguments:

```sql
SELECT CONCAT_WS('', 123, 456, 3.14) AS myconcat;
```

This will return `'1234563.14'`. In this case, the integers and float values are implicitly converted to strings before concatenation.

However, the CONCAT_WS function expects at least two arguments. If fewer than two arguments are passed to the CONCAT_WS function, it will throw an error:

```sql
SELECT CONCAT_WS() AS myconcat;
```

This will throw an error because the CONCAT_WS function expects at least two arguments.