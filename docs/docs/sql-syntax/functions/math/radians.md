# RADIANS

The `RADIANS` function is used to convert a given angle value from degrees to radians. It takes a single numeric argument (angle in degrees) and returns the angle in radians.

## Syntax

```sql
RADIANS(value)
```

- `value`: A numeric expression (angle in degrees) to be converted to radians.

## Examples

Let's consider a table named `SingleItem` with the following schema:

```sql
CREATE TABLE SingleItem (id FLOAT);
```

Insert a row into the `SingleItem` table:

```sql
INSERT INTO SingleItem VALUES (0);
```

### Example 1: Using RADIANS with float values

```sql
SELECT
RADIANS(180.0) as radians_1,
RADIANS(360.0) as radians_2
FROM SingleItem;
```

Result:

```
    radians_1 | radians_2
-------------+-------------
     3.141593 |  6.283185
```

### Example 2: Using RADIANS with integer values

```sql
SELECT RADIANS(90) as radians_with_int FROM SingleItem;
```

Result:

```
radians_with_int
-----------------
     1.570796
```

### Example 3: Using RADIANS with zero

```sql
SELECT RADIANS(0) as radians_with_zero FROM SingleItem;
```

Result:

```
radians_with_zero
------------------
         0.0
```

## Errors

The `RADIANS` function requires a numeric value as its argument. Using non-numeric values or more than one argument will result in an error.

### Example 4: Using RADIANS with non-numeric values

```sql
SELECT RADIANS('string') AS radians FROM SingleItem;
```

Error: Function requires a numeric value.

### Example 5: Using RADIANS with multiple arguments

```sql
SELECT RADIANS(0, 0) as radians_arg2 FROM SingleItem;
```

Error: Function expects 1 argument, but 2 were provided.