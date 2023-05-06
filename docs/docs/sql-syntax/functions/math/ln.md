# LN

The `LN` function is used to calculate the natural logarithm (base `e`) of a number. It takes a single FLOAT or INTEGER argument and returns a FLOAT value representing the natural logarithm of the given number.

## Example
The following example demonstrates the usage of the `LN` function in a SQL query:

```sql
CREATE TABLE SingleItem (id INTEGER DEFAULT LN(10));

INSERT INTO SingleItem VALUES (0);

SELECT
    LN(64.0) as ln1,
    LN(0.04) as ln2
FROM SingleItem;
```

This will return the following result:

```
ln1     | ln2
--------+-------------------
4.1589  | -3.2189
```

## Errors
1. If the argument is not of FLOAT or INTEGER type, a `FunctionRequiresFloatValue` error will be raised.
2. If the number of arguments provided to the function is not equal to 1, a `FunctionArgsLengthNotMatching` error will be raised.