# RPAD

The `RPAD` function in SQL pads the right side of a string with a specific set of characters.

## Syntax

```sql
RPAD(string, length, pad_string)
```

## Parameters

- `string`: The original string to pad.
- `length`: The length of the resulting string after padding. If this is less than the length of the original string, the result is truncated.
- `pad_string` (optional): The string to use for padding. If not supplied, spaces are used.

## Return Value

The function returns a new string that is the same as the original string, but with additional padding on the right side to achieve the specified length.

## Errors

- If the `string` argument is not a string, a `FunctionRequiresStringValue` error will be returned.
- If the `length` argument is not a positive integer, a `FunctionRequiresUSizeValue` error will be returned.

## Examples

Consider a table `Item` created and filled with the following data:

```sql
CREATE TABLE Item (
    name TEXT
);
INSERT INTO Item VALUES ('hello');
```

You can use the `RPAD` function to pad the `name` values to a length of 10 with the character 'b':

```sql
SELECT RPAD(name, 10, 'b') AS padded_name FROM Item;
```

This will return:

```
hellobbbbb
```

If the `length` argument is less than the length of the string, the original string will be truncated:

```sql
SELECT RPAD(name, 3, 'b') AS padded_name FROM Item;
```

This will return:

```
hel
```