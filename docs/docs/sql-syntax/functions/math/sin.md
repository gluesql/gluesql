# SIN

The `SIN` function is used to calculate the sine of a number. It takes a single numeric argument (angle in radians) and returns the sine of that angle.

## Syntax

```sql
SIN(value)
```

- `value`: A numeric expression (angle in radians) for which the sine is to be calculated.

## Examples

Let's consider a table named `SingleItem` with the following schema:

```sql
CREATE TABLE SingleItem (id INTEGER);
```

Insert a row into the `SingleItem` table:

```sql
INSERT INTO SingleItem VALUES (0);
```

### Example 1: Using SIN with float values

```sql
SELECT SIN(0.5) AS sin1, SIN(1) AS sin2 FROM SingleItem;
```

Result:

```
    sin1     |    sin2
-------------+--------------
 0.479425539 | 0.841470984
```

### Example 2: Using SIN with NULL values

```sql
SELECT SIN(NULL) AS sin FROM SingleItem;
```

Result:

```
  sin
-------
 (null)
```

## Errors

The `SIN` function requires a numeric value as its argument. Using non-numeric values or more than one argument will result in an error.

### Example 3: Using SIN with non-numeric values

```sql
SELECT SIN('string') AS sin FROM SingleItem;
```

Error: Function requires a numeric value.

### Example 4: Using SIN with multiple arguments

```sql
SELECT SIN(1.0, 2.0) AS sin FROM SingleItem;
```

Error: Function expects 1 argument, but 2 were provided.