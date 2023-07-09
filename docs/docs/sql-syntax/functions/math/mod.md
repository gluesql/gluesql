# MOD

The `MOD` function is used to calculate the remainder of a division operation. It takes two arguments (a dividend and a divisor) and returns the remainder of the division operation. Both dividend and divisor can be FLOAT or INTEGER type. The return type of the function is FLOAT.

## Example
The following example demonstrates the usage of the `MOD` function in a SQL query:

```sql
CREATE TABLE FloatDiv (
    dividend FLOAT DEFAULT MOD(30, 11),
    divisor FLOAT DEFAULT DIV(3, 2)
);

INSERT INTO FloatDiv (dividend, divisor) VALUES (12.0, 3.0), (12.34, 56.78), (-12.3, 4.0);

SELECT MOD(dividend, divisor) FROM FloatDiv;
```

This will return the following result:

```
MOD(dividend, divisor)
0.0
12.34
-0.3
```

## Errors
1. If the divisor is zero, a `DivisorShouldNotBeZero` error will be raised.
2. If either of the arguments is not of FLOAT or INTEGER type, a `FunctionRequiresFloatOrIntegerValue` error will be raised.
3. If the number of arguments provided to the function is not equal to 2, a `FunctionArgsLengthNotMatching` error will be raised.