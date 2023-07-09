# EXP

The `EXP` function is used to calculate the exponential value of a number. It takes a single FLOAT or INTEGER argument and returns a FLOAT value representing the exponential value of the given number.

## Example
The following example demonstrates the usage of the `EXP` function in a SQL query:

```sql
SELECT
    EXP(2.0) as exp1,
    EXP(5.5) as exp2;
```

This will return the following result:

```
exp1           | exp2
---------------+-------------------
2.0_f64.exp()  | 5.5_f64.exp()
```

## Errors
1. If the argument is not of FLOAT or INTEGER type, a `FunctionRequiresFloatValue` error will be raised.
2. If the number of arguments provided to the function is not equal to 1, a `FunctionArgsLengthNotMatching` error will be raised.