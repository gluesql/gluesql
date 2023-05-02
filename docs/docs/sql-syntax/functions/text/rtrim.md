# RTRIM

The `RTRIM` function in SQL removes characters from the right (trailing side) of a string.

## Syntax

```sql
RTRIM(string, trim_string)
```

## Parameters

- `string`: The original string to trim.
- `trim_string` (optional): The characters to remove from the string. If not supplied, spaces are removed.

## Return Value

The function returns a new string that is the same as the original string, but without the specified trailing characters.

## Errors

- If the `string` or `trim_string` argument is not a string, a `FunctionRequiresStringValue` error will be returned.

## Examples

Consider a table `Item` created and filled with the following data:

```sql
CREATE TABLE Item (
    name TEXT
);
INSERT INTO Item VALUES ('testxxzx ');
```

You can use the `RTRIM` function to remove trailing spaces from the `name` values:

```sql
SELECT RTRIM(name) AS trimmed_name FROM Item;
```

This will return:

```
testxxzx
```

You can also specify a string of characters to remove. The function will remove any character in this string from the end of the original string:

```sql
SELECT RTRIM(name, 'zx ') AS trimmed_name FROM Item;
```

This will return:

```
test
```