# LOG

The `LOG` function calculates the logarithm of a number with a specified base. It takes two FLOAT or INTEGER arguments and returns a FLOAT value representing the logarithm of the first argument with the base specified by the second argument.

## Example
The following example demonstrates the usage of the `LOG` function in a SQL query:

```sql
CREATE TABLE SingleItem (id INTEGER DEFAULT LOG(2, 64));

INSERT INTO SingleItem VALUES (0);

SELECT
    LOG(64.0, 2.0) as log_1,
    LOG(0.04, 10.0) as log_2
FROM SingleItem;
```

This will return the following result:

```
log_1 | log_2
------+-------------------
6.0   | -1.39794
```

## Errors
1. If either of the arguments is not of FLOAT or INTEGER type, a `FunctionRequiresFloatValue` error will be raised.
2. If the number of arguments provided to the function is not equal to 2, a `FunctionArgsLengthNotMatching` error will be raised.