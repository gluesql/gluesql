# FLOOR

The `FLOOR` function is used to round a number down to the nearest integer value. It takes a single floating-point or integer value as its argument and returns a floating-point value.

## Syntax

```sql
FLOOR(value)
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

### Example 1: Using FLOOR function

```sql
SELECT
FLOOR(0.3) as floor1,
FLOOR(-0.8) as floor2,
FLOOR(10) as floor3,
FLOOR(6.87421) as floor4
FROM SingleItem;
```

Result:

```
floor1 | floor2 | floor3 | floor4
-------+--------+--------+--------
   0.0 |   -1.0 |   10.0 |    6.0
```

Note that the returned values are floating-point numbers, even though they represent integer values.

## Errors

The `FLOOR` function expects a floating-point or integer value as its argument. Providing any other type, such as a string or boolean, will result in an error.

### Example 2: Using FLOOR with a string argument

```sql
SELECT FLOOR('string') AS floor FROM SingleItem;
```

Error: Function requires a floating-point or integer value.

### Example 3: Using FLOOR with a boolean argument

```sql
SELECT FLOOR(TRUE) AS floor FROM SingleItem;
```

Error: Function requires a floating-point or integer value.