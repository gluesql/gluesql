# LCM

The `LCM` function is used to find the least common multiple (LCM) of two integers. It takes two INTEGER arguments and returns an INTEGER value representing the least common multiple of the given integers.

## Example
The following example demonstrates the usage of the `LCM` function in a SQL query:

```sql
CREATE TABLE LcmI64 (
    left INTEGER NULL,
    right INTEGER NULL
);

INSERT INTO LcmI64 VALUES (0, 3), (2, 4), (6, 8), (3, 5), (1, NULL), (NULL, 1);

SELECT LCM(left, right) AS test FROM LcmI64;
```

This will return the following result:

```
test
0
4
24
15
NULL
NULL
```

## Errors
1. If either of the arguments is not of INTEGER type, a `FunctionRequiresIntegerValue` error will be raised.
2. If the number of arguments provided to the function is not equal to 2, a `FunctionArgsLengthNotMatching` error will be raised.