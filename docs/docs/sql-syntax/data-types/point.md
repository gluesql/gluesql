# POINT

The `POINT` data type stores a two-dimensional point with `x` and `y` coordinates.

## Creating a Table with a POINT Column

```sql
CREATE TABLE PointGroup (
    point_field POINT
);
```

## Inserting POINT Values

Use the `POINT` function to create a point value:

```sql
INSERT INTO PointGroup VALUES (POINT(0.3134, 0.156));
```

## Querying POINT Values

```sql
SELECT point_field FROM PointGroup;
```

## Updating and Deleting POINT Values

```sql
UPDATE PointGroup
SET point_field = POINT(2.0, 1.0)
WHERE point_field = POINT(0.3134, 0.156);

DELETE FROM PointGroup
WHERE point_field = POINT(2.0, 1.0);
```

## Casting Text to POINT

```sql
SELECT CAST('POINT(-71.064544 42.28787)' AS POINT) AS pt;
```

## Related Functions

- `POINT(x, y)` creates a point value.
- `GET_X(point)` returns the x-coordinate.
- `GET_Y(point)` returns the y-coordinate.
- `CALC_DISTANCE(point1, point2)` returns the distance between two points.
