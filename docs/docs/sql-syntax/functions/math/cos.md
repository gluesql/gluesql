# COS

The `COS` function is used to calculate the cosine of a number. It takes a single numeric argument (angle in radians) and returns the cosine of that angle.

## Syntax

```sql
COS(value)
```

- `value`: A numeric expression (angle in radians) for which the cosine is to be calculated.

## Examples

Let's consider a table named `SingleItem` with the following schema:

```sql
CREATE TABLE SingleItem (id INTEGER);
```

Insert a row into the `SingleItem` table:

```sql
INSERT INTO SingleItem VALUES (0);
```

### Example 1: Using COS with float values

```sql
SELECT COS(0.5) AS cos1, COS(1) AS cos2 FROM SingleItem;
```

Result:

```
    cos1     |    cos2
-------------+--------------
 0.877582562 | 0.540302306
```

### Example 2: Using COS with NULL values

```sql
SELECT COS(NULL) AS cos FROM SingleItem;
```

Result:

```
  cos
-------
 (null)
```

## Errors

The `COS` function requires a numeric value as its argument. Using non-numeric values or more than one argument will result in an error.

### Example 3: Using COS with non-numeric values

```sql
SELECT COS('string') AS cos FROM SingleItem;
```

Error: Function requires a numeric value.

### Example 4: Using COS with multiple arguments

```sql
SELECT COS(1.0, 2.0) AS cos FROM SingleItem;
```

Error: Function expects 1 argument, but 2 were provided.