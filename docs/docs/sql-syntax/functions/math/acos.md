# ACOS

The `ACOS` function is used to calculate the arccosine (inverse cosine) of a number. It takes a single numeric argument, which should be a float value in the range of -1 to 1, and returns the arccosine of that number in radians.

## Syntax

```sql
ACOS(value)
```

- `value`: A numeric expression for which the arccosine is to be calculated. The value should be in the range of -1 to 1.

## Examples

Let's consider a table named `SingleItem` with the following schema:

```sql
CREATE TABLE SingleItem (id INTEGER);
```

Insert a row into the `SingleItem` table:

```sql
INSERT INTO SingleItem VALUES (0);
```

### Example 1: Using ACOS with float values

```sql
SELECT ACOS(0.5) AS acos1, ACOS(1) AS acos2 FROM SingleItem;
```

Result:

```
     acos1      |     acos2
----------------+---------------
 1.0471975511966 | 0.0
```

### Example 2: Using ACOS with NULL values

```sql
SELECT ACOS(NULL) AS acos FROM SingleItem;
```

Result:

```
  acos
-------
 (null)
```

## Errors

The `ACOS` function requires a numeric value in the range of -1 to 1 as its argument. Using non-numeric values or more than one argument will result in an error.

### Example 3: Using ACOS with non-numeric values

```sql
SELECT ACOS('string') AS acos FROM SingleItem;
```

Error: Function requires a numeric value.

### Example 4: Using ACOS with multiple arguments

```sql
SELECT ACOS(1.0, 2.0) AS acos FROM SingleItem;
```

Error: Function expects 1 argument, but 2 were provided.