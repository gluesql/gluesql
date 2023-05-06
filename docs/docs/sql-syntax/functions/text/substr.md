# SUBSTR

The `SUBSTR` function in SQL is used to extract a substring from a string.

## Syntax

```sql
SUBSTR(string, start_position, length)
```

## Parameters

- `string`: The original string.
- `start_position`: The position in the string where the extraction of the substring will begin. The position of the first character is 1. If `start_position` is 0 or negative, the function treats it as 1.
- `length` (optional): The number of characters to extract. If `length` is not included, the function will return all characters starting from `start_position`.

## Return Value

The function returns a string which is a substring of the original string. The substring starts at `start_position` and has `length` number of characters.

## Errors

- If the `string` parameter is not a string value, a `EvaluateError::FunctionRequiresStringValue` error will be returned.
- If the `start_position` or `length` parameters are not integer values, a `EvaluateError::FunctionRequiresIntegerValue` error will be returned.
- If the `length` parameter is negative, a `EvaluateError::NegativeSubstrLenNotAllowed` error will be returned.

## Examples

Consider a table `Item` created and filled with the following data:

```sql
CREATE TABLE Item (name TEXT);
INSERT INTO Item VALUES ('Blop mc blee'), ('B'), ('Steven the &long named$ folken!');
```

You can use the `SUBSTR` function to get a substring from the `name` values:

```sql
SELECT SUBSTR(name, 2) AS test FROM Item;
```

This will return:

```
lop mc blee
(empty string)
teven the &long named$ folken!
```

The function takes the substring starting from the second character until the end for each `name` value.