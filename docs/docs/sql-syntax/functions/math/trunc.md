# TRUNC

The `TRUNC` function is used to truncate a number towards zero, removing the fractional part without rounding. It takes a single floating-point or integer value as its argument and returns a floating-point value.

## Syntax

```sql
TRUNC(value)
```

## Examples

Let's consider a table named `SingleItem` with the following schema:

```sql
CREATE TABLE SingleItem (id INTEGER);
```

Insert a row into the `SingleItem` table:

```sql
INSERT INTO SingleItem VALUES (0);
```

### Example 1: Using TRUNC function

```sql
SELECT TRUNC(0.3) AS trunc1, TRUNC(-0.8) AS trunc2, TRUNC(10) AS trunc3, TRUNC(6.87421) AS trunc4 FROM SingleItem;
```

Result:

```
trunc1 | trunc2 | trunc3 | trunc4
-------+--------+--------+--------
   0.0 |    0.0 |   10.0 |    6.0
```

Note that the returned values are floating-point numbers, even though they represent integer values. The `TRUNC` function truncates towards zero, which means:
- For positive numbers: removes the decimal part (6.87421 → 6.0)
- For negative numbers: truncates towards zero (-0.8 → 0.0, -1.8 → -1.0)

## Errors

The `TRUNC` function expects a floating-point or integer value as its argument. Providing any other type, such as a string or boolean, will result in an error.

### Example 2: Using TRUNC with a string argument

```sql
SELECT TRUNC('string') AS trunc FROM SingleItem;
```

Error: Function requires a floating-point or integer value.

### Example 3: Using TRUNC with a boolean argument

```sql
SELECT TRUNC(TRUE) AS trunc FROM SingleItem;
```

Error: Function requires a floating-point or integer value.
