# ROUND

The `ROUND` function is used to round a number to the nearest integer value. It takes a single floating-point or integer value as its argument and returns a floating-point value.

## Syntax

```sql
ROUND(value)
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

### Example 1: Using ROUND function

```sql
SELECT ROUND(0.3) AS round1,
ROUND(-0.8) AS round2,
ROUND(10) AS round3,
ROUND(6.87421) AS round4
FROM SingleItem;
```

Result:

```
round1 | round2 | round3 | round4
-------+--------+--------+--------
   0.0 |   -1.0 |   10.0 |    7.0
```

Note that the returned values are floating-point numbers, even though they represent integer values.

## Errors

The `ROUND` function expects a floating-point or integer value as its argument. Providing any other type, such as a string or boolean, will result in an error.

### Example 2: Using ROUND with a string argument

```sql
SELECT ROUND('string') AS round FROM SingleItem;
```

Error: Function requires a floating-point or integer value.

### Example 3: Using ROUND with a boolean argument

```sql
SELECT ROUND(TRUE) AS round FROM SingleItem;
```

Error: Function requires a floating-point or integer value.