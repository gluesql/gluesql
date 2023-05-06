# LOG10

The `LOG10` function is used to calculate the base-10 logarithm of a number. It takes a single FLOAT or INTEGER argument and returns a FLOAT value representing the base-10 logarithm of the given number.

## Example
The following example demonstrates the usage of the `LOG10` function in a SQL query:

```sql
CREATE TABLE SingleItem (id INTEGER DEFAULT LOG10(100));

INSERT INTO SingleItem VALUES (0);

SELECT
    LOG10(64.0) as log10_1,
    LOG10(0.04) as log10_2
FROM SingleItem;
```

This will return the following result:

```
log10_1 | log10_2
--------+-------------------
1.8062  | -1.3979
```

## Errors
1. If the argument is not of FLOAT or INTEGER type, a `FunctionRequiresFloatValue` error will be raised.
2. If the number of arguments provided to the function is not equal to 1, a `FunctionArgsLengthNotMatching` error will be raised.