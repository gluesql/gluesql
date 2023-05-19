# REPEAT

The `REPEAT` function in SQL is used to repeat a string for a specified number of times.

## Syntax

```sql
REPEAT(string, number)
```

## Parameters

- `string`: The string to be repeated.
- `number`: The number of times to repeat the string. 

## Return Value

The function returns a string which is the concatenation of the input string repeated the specified number of times.

## Errors

- If the parameters are not in the correct format, a `TranslateError::FunctionArgsLengthNotMatching` error will be returned. This function requires exactly two arguments.
- If either `string` or `number` are not string values, a `EvaluateError::FunctionRequiresStringValue` error will be returned.

## Examples

Consider a table `Item` created and filled with the following data:

```sql
CREATE TABLE Item (name TEXT);
INSERT INTO Item VALUES ('hello');
```

You can use the `REPEAT` function to repeat the `name` values:

```sql
SELECT REPEAT(name, 2) AS test FROM Item;
```

This will return:

```
hellohello
```

The 'hello' string is repeated twice as specified by the second parameter to the `REPEAT` function.