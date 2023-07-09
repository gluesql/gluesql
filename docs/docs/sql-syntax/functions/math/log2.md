# LOG2

The `LOG2` function is used to calculate the base-2 logarithm of a number. It takes a single FLOAT or INTEGER argument and returns a FLOAT value representing the base-2 logarithm of the given number.

## Example
The following example demonstrates the usage of the `LOG2` function in a SQL query:

```sql
CREATE TABLE SingleItem (id INTEGER DEFAULT LOG2(1024));

INSERT INTO SingleItem VALUES (0);

SELECT
    LOG2(64.0) as log2_1,
    LOG2(0.04) as log2_2
FROM SingleItem;
```

This will return the following result:

```
log2_1 | log2_2
-------+-------------------
6.0    | -4.5850
```

## Errors
1. If the argument is not of FLOAT or INTEGER type, a `FunctionRequiresFloatValue` error will be raised.
2. If the number of arguments provided to the function is not equal to 1, a `FunctionArgsLengthNotMatching` error will be raised.