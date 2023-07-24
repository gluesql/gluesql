# GCD

The `GCD` function is used to find the greatest common divisor (GCD) of two integers. It takes two INTEGER arguments and returns an INTEGER value representing the greatest common divisor of the given integers.

## Example
The following example demonstrates the usage of the `GCD` function in a SQL query:

```sql
CREATE TABLE GcdI64 (
    left INTEGER NULL DEFAULT GCD(3, 4),
    right INTEGER NULL
);

INSERT INTO GcdI64 VALUES (0, 3), (2, 4), (6, 8), (3, 5), (1, NULL), (NULL, 1);

SELECT GCD(left, right) AS test FROM GcdI64;
```

This will return the following result:

```
test
3
2
2
1
NULL
NULL
```

## Errors
1. If either of the arguments is not of INTEGER type, a `FunctionRequiresIntegerValue` error will be raised.
2. If the number of arguments provided to the function is not equal to 2, a `FunctionArgsLengthNotMatching` error will be raised.
3. If either of the arguments is the minimum i64 value (`-9223372036854775808`), an overflow occurs when attempting to take the absolute value. In this case, a `GcdOverflowError` is raised.