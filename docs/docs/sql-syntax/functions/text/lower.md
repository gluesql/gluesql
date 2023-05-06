# LOWER

The `LOWER` function in SQL returns a string in which all alphabetic characters in a specified string are converted to lowercase.

## Syntax

```sql
LOWER(string)
```

## Parameters

- `string`: The original string to convert.

## Return Value

The function returns a new string that is the same as the original string, but with all uppercase characters converted to lowercase. Non-alphabetic characters in the string are unaffected.

## Errors

- If the `string` argument is not a string, a `FunctionRequiresStringValue` error will be returned.

## Examples

Consider a table `Item` created and filled with the following data:

```sql
CREATE TABLE Item (
    name TEXT
);
INSERT INTO Item VALUES ('ABCD'), ('Abcd'), ('abcd');
```

You can use the `LOWER` function to convert all `name` values to lowercase:

```sql
SELECT LOWER(name) AS lower_name FROM Item;
```

This will return:

```
abcd
abcd
abcd
```

Note that the `LOWER` function affects only alphabetic characters. Non-alphabetic characters in the string remain unchanged.