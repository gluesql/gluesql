# DEGREES

The `DEGREES` function is used to convert a given angle value from radians to degrees. It takes a single numeric argument (angle in radians) and returns the angle in degrees.

## Syntax

```sql
DEGREES(value)
```

- `value`: A numeric expression (angle in radians) to be converted to degrees.

## Examples

Let's consider a table named `SingleItem` with the following schema:

```sql
CREATE TABLE SingleItem (id FLOAT);
```

Insert a row into the `SingleItem` table:

```sql
INSERT INTO SingleItem VALUES (0);
```

### Example 1: Using DEGREES with float values

```sql
SELECT
DEGREES(180.0) as degrees_1,
DEGREES(360.0) as degrees_2
FROM SingleItem;
```

Result:

```
   degrees_1 | degrees_2
-------------+-------------
10313.240312 | 20626.480624
```

### Example 2: Using DEGREES with integer values

```sql
SELECT DEGREES(90) as degrees_with_int FROM SingleItem;
```

Result:

```
degrees_with_int
-----------------
     5156.620156
```

### Example 3: Using DEGREES with zero

```sql
SELECT DEGREES(0) as degrees_with_zero FROM SingleItem;
```

Result:

```
degrees_with_zero
------------------
         0.0
```

## Errors

The `DEGREES` function requires a numeric value as its argument. Using non-numeric values or more than one argument will result in an error.

### Example 4: Using DEGREES with non-numeric values

```sql
SELECT DEGREES('string') AS degrees FROM SingleItem;
```

Error: Function requires a numeric value.

### Example 5: Using DEGREES with multiple arguments

```sql
SELECT DEGREES(0, 0) as degrees_arg2 FROM SingleItem;
```

Error: Function expects 1 argument, but 2 were provided.