# REVERSE

The `REVERSE` function in SQL is used to reverse a string.

## Syntax

```sql
REVERSE(string)
```

## Parameters

- `string`: The string to be reversed.

## Return Value

The function returns a string which is the reverse of the input string.

## Errors

If the parameter is not a string value, a `EvaluateError::FunctionRequiresStringValue` error will be returned.

## Examples

Consider a table `Item` created and filled with the following data:

```sql
CREATE TABLE Item (name TEXT);
INSERT INTO Item VALUES ('Let''s meet');
```

You can use the `REVERSE` function to reverse the `name` values:

```sql
SELECT REVERSE(name) AS test FROM Item;
```

This will return:

```
teem s'teL
```

The 'Let''s meet' string is reversed as 'teem s'teL'.