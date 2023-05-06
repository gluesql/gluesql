
# POINT

The `POINT` function creates a point value using the provided x and y coordinates. A point value represents a two-dimensional geometric point with a pair of floating-point numbers, often used for storing spatial data.

## Syntax

```sql
POINT(x, y)
```

## Examples

Create a table with a `POINT` data type column:

```sql
CREATE TABLE Foo (point_field POINT);
```

Insert a record with a point value:

```sql
INSERT INTO Foo VALUES (POINT(0.3134, 0.156));
```

Select the `point_field` column:

```sql
SELECT point_field AS point_field FROM Foo;
```

Update the `point_field` column:

```sql
UPDATE Foo SET point_field = POINT(2.0, 1.0) WHERE point_field = POINT(0.3134, 0.156);
```

Select the updated `point_field` column:

```sql
SELECT point_field AS point_field FROM Foo;
```

Delete the record with the specified point value:

```sql
DELETE FROM Foo WHERE point_field = POINT(2.0, 1.0);
```

Casting a string to a `POINT`:

```sql
SELECT CAST('POINT(-71.064544 42.28787)' AS POINT) AS pt;
```