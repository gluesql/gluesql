# ATAN

The `ATAN` function is used to calculate the arctangent (inverse tangent) of a number. It takes a single numeric argument, and returns the arctangent of that number in radians.

## Syntax

```sql
ATAN(value)
```

- `value`: A numeric expression for which the arctangent is to be calculated.

## Examples

Let's consider a table named `SingleItem` with the following schema:

```sql
CREATE TABLE SingleItem (id INTEGER);
```

Insert a row into the `SingleItem` table:

```sql
INSERT INTO SingleItem VALUES (0);
```

### Example 1: Using ATAN with float values

```sql
SELECT ATAN(0.5) AS atan1, ATAN(1) AS atan2 FROM SingleItem;
```

Result:

```
    atan1     |    atan2
--------------+---------------
 0.463647609  | 0.785398163
```

### Example 2: Using ATAN with NULL values

```sql
SELECT ATAN(NULL) AS atan FROM SingleItem;
```

Result:

```
  atan
-------
 (null)
```

## Errors

The `ATAN` function requires a numeric value as its argument. Using non-numeric values or more than one argument will result in an error.

### Example 3: Using ATAN with non-numeric values

```sql
SELECT ATAN('string') AS atan FROM SingleItem;
```

Error: Function requires a numeric value.

### Example 4: Using ATAN with multiple arguments

```sql
SELECT ATAN(1.0, 2.0) AS atan FROM SingleItem;
```

Error: Function expects 1 argument, but 2 were provided.