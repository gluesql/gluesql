# DIV

The `DIV` function is used to perform integer division. It takes two arguments (a dividend and a divisor) and returns the integer quotient of the division operation. Both dividend and divisor can be FLOAT or INTEGER type. The return type of the function is INTEGER.

## Example
The following example demonstrates the usage of the `DIV` function in a SQL query:

```sql
CREATE TABLE FloatDiv (
    dividend FLOAT DEFAULT DIV(30, 11),
    divisor FLOAT DEFAULT DIV(3, 2)
);

INSERT INTO FloatDiv (dividend, divisor) VALUES (12.0, 3.0), (12.34, 56.78), (-12.3, 4.0);

SELECT DIV(dividend, divisor) FROM FloatDiv;
```

This will return the following result:

```
DIV(dividend, divisor)
4
0
-4
```

## Errors
1. If the divisor is zero, a `DivisorShouldNotBeZero` error will be raised.
2. If either of the arguments is not of FLOAT or INTEGER type, a `FunctionRequiresFloatOrIntegerValue` error will be raised.
3. If the number of arguments provided to the function is not equal to 2, a `FunctionArgsLengthNotMatching` error will be raised.