# GET_Y

The `GET_Y` function returns the y-coordinate of a given `POINT` data type. It takes one `POINT` data type argument and returns a `FLOAT` value representing the y-coordinate.

## Syntax

```sql
GET_Y(point)
```

**Parameters:**

- `point`: The geographical coordinate of type `Point` from which the Y-coordinate will be extracted.

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

### Example 1: Get the Y-coordinate from a point

```sql
SELECT GET_Y(point_field) AS point_field FROM PointGroup;
```

**Result:**

| point_field |
|-------------|
| 0.156       |

### Example 2: Get the Y-coordinate from a point using CAST

```sql
SELECT GET_Y(CAST('POINT(0.1 -0.2)' AS POINT)) AS ptx;
```

**Result:**

| ptx    |
|--------|
| -0.2   |

### Example 3: Get the Y-coordinate from a point using POINT function

```sql
SELECT GET_Y(POINT(0.1, -0.2)) AS ptx;
```

**Result:**

| ptx    |
|--------|
| -0.2   |

## Errors

If the argument is not of type `Point`, a `FunctionRequiresPointValue` error will be thrown.
