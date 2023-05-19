# RIGHT

The `RIGHT` function in SQL returns the specified number of characters from the end (right side) of a given string.

## Syntax

```sql
RIGHT(string, number)
```

## Parameters

- `string`: The original string from which to extract characters.
- `number`: The number of characters to extract from the end of the string. This must be an integer.

## Return Value

The function returns a string, which consists of the specified number of characters from the end of the original string. If the original string is shorter than the specified number, the function returns the whole string.

## Errors

- If the `number` argument is not an integer, a `FunctionRequiresIntegerValue` error will be returned.
- If the `string` argument is not a string, a `FunctionRequiresStringValue` error will be returned.
- If the `number` argument is negative, a `FunctionRequiresUSizeValue` error will be returned.

## Examples

Consider a table `Item` created and filled with the following data:

```sql
CREATE TABLE Item (
    name TEXT
);
INSERT INTO Item VALUES ('Blop mc blee'), ('B'), ('Steven the &long named$ folken!');
```

You can use the `RIGHT` function to extract the last three characters of each `name`:

```sql
SELECT RIGHT(name, 3) AS test FROM Item;
```

This will return:

```
lee
B
en!
```

Note that when the string length is less than the specified number (as with 'B'), the function will return the whole string.
