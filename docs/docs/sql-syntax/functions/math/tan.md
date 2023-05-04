# TAN

The `TAN` function is used to calculate the tangent of a number. It takes a single numeric argument (angle in radians) and returns the tangent of that angle.

## Syntax

```sql
TAN(value)
```

- `value`: A numeric expression (angle in radians) for which the tangent is to be calculated.

## Examples

Let's consider a table named `SingleItem` with the following schema:

```sql
CREATE TABLE SingleItem (id INTEGER);
```

Insert a row into the `SingleItem` table:

```sql
INSERT INTO SingleItem VALUES (0);
```

### Example 1: Using TAN with float values

```sql
SELECT TAN(0.5) AS tan1, TAN(1) AS tan2 FROM SingleItem;
```

Result:

```
    tan1    |    tan2
------------+---------------
 0.54630249 | 1.557407725
```

### Example 2: Using TAN with NULL values

```sql
SELECT TAN(NULL) AS tan FROM SingleItem;
```

Result:

```
  tan
-------
 (null)
```

## Errors

The `TAN` function requires a numeric value as its argument. Using non-numeric values or more than one argument will result in an error.

### Example 3: Using TAN with non-numeric values

```sql
SELECT TAN('string') AS tan FROM SingleItem;
```

Error: Function requires a numeric value.

### Example 4: Using TAN with multiple arguments

```sql
SELECT TAN(1.0, 2.0) AS tan FROM SingleItem;
```

Error: Function expects 1 argument, but 2 were provided.