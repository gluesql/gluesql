# CEIL

The `CEIL` function is used to round a number up to the nearest integer value. It takes a single floating-point or integer value as its argument and returns a floating-point value.

## Syntax

```sql
CEIL(value)
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

### Example 1: Using CEIL function

```sql
SELECT CEIL(0.3) AS ceil1,
CEIL(-0.8) AS ceil2,
CEIL(10) AS ceil3,
CEIL(6.87421) AS ceil4
FROM SingleItem;
```

Result:

```
ceil1 | ceil2 | ceil3 | ceil4
------+-------+-------+-------
  1.0 |   0.0 |  10.0 |   7.0
```

Note that the returned values are floating-point numbers, even though they represent integer values.

## Errors

The `CEIL` function expects a floating-point or integer value as its argument. Providing any other type, such as a string or boolean, will result in an error.

### Example 2: Using CEIL with a string argument

```sql
SELECT CEIL('string') AS ceil FROM SingleItem;
```

Error: Function requires a floating-point or integer value.

### Example 3: Using CEIL with a boolean argument

```sql
SELECT CEIL(TRUE) AS ceil FROM SingleItem;
```

Error: Function requires a floating-point or integer value.