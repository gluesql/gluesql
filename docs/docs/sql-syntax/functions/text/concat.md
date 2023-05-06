# CONCAT

The CONCAT function in SQL concatenates two or more strings into one string.

## Syntax

The syntax for the CONCAT function in SQL is:

```sql
CONCAT ( string1, string2, ..., stringN )
```

## Parameters

- `string1`, `string2`, ..., `stringN`: These are the strings that you wish to concatenate together. 

## Examples

Let's consider a few examples to understand how to use the CONCAT function.

To concatenate two strings:

```sql
SELECT CONCAT('ab', 'cd') AS myconcat;
```

This will return `'abcd'`.

You can also concatenate more than two strings:

```sql
SELECT CONCAT('ab', 'cd', 'ef') AS myconcat;
```

This will return `'abcdef'`.

If any string in the CONCAT function is NULL, the function will return NULL:

```sql
SELECT CONCAT('ab', 'cd', NULL, 'ef') AS myconcat;
```

This will return NULL.

The CONCAT function can also take non-string arguments:

```sql
SELECT CONCAT(123, 456, 3.14) AS myconcat;
```

This will return `'1234563.14'`. In this case, the integers and float values are implicitly converted to strings before concatenation.

However, the CONCAT function expects at least one argument. If no arguments are passed to the CONCAT function, it will throw an error:

```sql
SELECT CONCAT() AS myconcat;
```

This will throw an error because the CONCAT function expects at least one argument.