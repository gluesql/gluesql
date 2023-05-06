# GET_X

The `GET_X` function returns the x-coordinate of a given `POINT` data type. It takes one `POINT` data type argument and returns a `FLOAT` value representing the x-coordinate.

## Syntax

```sql
GET_X(point)
```

**Parameters:**

- `point`: The geographical coordinate of type `Point` from which the X-coordinate will be extracted.

## Examples

Consider the following table `PointGroup`:

```sql
CREATE TABLE PointGroup (
    point_field POINT
);
```

With the following data:

```sql
INSERT INTO PointGroup VALUES (POINT(0.3134, 0.156));
```

### Example 1: Get the X-coordinate from a point

```sql
SELECT GET_X(point_field) AS point_field FROM PointGroup;
```

**Result:**

| point_field |
|-------------|
| 0.3134      |

### Example 2: Get the X-coordinate from a point using CAST

```sql
SELECT GET_X(CAST('POINT(0.1 -0.2)' AS POINT)) AS ptx;
```

**Result:**

| ptx   |
|-------|
| 0.1   |

### Example 3: Get the X-coordinate from a point using POINT function

```sql
SELECT GET_X(POINT(0.1, -0.2)) AS ptx;
```

**Result:**

| ptx   |
|-------|
| 0.1   |

## Errors

If the argument is not of type `Point`, a `FunctionRequiresPointValue` error will be thrown.
