# POSITION

The `POSITION` function in SQL is used to find the position of a substring in a string. The position of the first occurrence of the substring is returned. If the substring is not found, this function returns 0.

## Syntax

```sql
POSITION(substring IN string)
```

## Parameters

- `substring`: The substring to search for.
- `string`: The string in which to search.

## Return Value

The function returns an integer representing the position of the first occurrence of the substring in the string, starting from 1. If the substring is not found, the function returns 0.

## Errors

- If either `substring` or `string` are not string values, a `ValueError::NonStringParameterInPosition` error will be returned.

## Examples

Consider a table `Food` created and filled with the following data:

```sql
CREATE TABLE Food (
    name TEXT
);
INSERT INTO Food VALUES ('pork');
INSERT INTO Food VALUES ('burger');
```

You can use the `POSITION` function to find the position of a substring within the `name` values:

```sql
SELECT POSITION('e' IN name) AS test FROM Food;
```

This will return:

```
0
5
```

The first 'e' in 'burger' is at position 5, so the function returns 5 for 'burger'. There is no 'e' in 'pork', so the function returns 0 for 'pork'.