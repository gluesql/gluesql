# UPPER

The `UPPER` function in SQL converts all lowercase alphabetic characters in a specified string to uppercase.

## Syntax

```sql
UPPER(string)
```

## Parameters

- `string`: The original string to convert.

## Return Value

The function returns a new string that is the same as the original string, but with all lowercase characters converted to uppercase. Non-alphabetic characters in the string are unaffected.

## Errors

- If the `string` argument is not a string, a `FunctionRequiresStringValue` error will be returned.

## Examples

Consider a table `Item` created and filled with the following data:

```sql
CREATE TABLE Item (
    name TEXT
);
INSERT INTO Item VALUES ('abcd'), ('Abcd'), ('ABCD');
```

You can use the `UPPER` function to convert all `name` values to uppercase:

```sql
SELECT UPPER(name) AS upper_name FROM Item;
```

This will return:

```
ABCD
ABCD
ABCD
```

Note that the `UPPER` function affects only alphabetic characters. Non-alphabetic characters in the string remain unchanged.