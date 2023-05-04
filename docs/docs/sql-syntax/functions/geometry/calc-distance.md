# CALC_DISTANCE

The `CALC_DISTANCE` function calculates the Euclidean distance between two points in a two-dimensional plane. It takes two `POINT` data type arguments and returns a `FLOAT` value representing the distance between the two points.

## Syntax

```sql
CALC_DISTANCE(point1, point2)
```

## Examples

Create a table with `POINT` data type columns:

```sql
CREATE TABLE Foo (id FLOAT, geo1 POINT, geo2 POINT, bar FLOAT);
```

Insert a record with point values:

```sql
INSERT INTO Foo VALUES (1, POINT(0.3134, 3.156), POINT(1.415, 3.231), 3);
```

Calculate the distance between two points:

```sql
SELECT CALC_DISTANCE(geo1, geo2) AS georesult FROM Foo;
```

Invalid use cases:

Trying to use `CALC_DISTANCE` with only one argument:

```sql
SELECT CALC_DISTANCE(geo1) AS georesult FROM Foo;
```

Using `CALC_DISTANCE` with non-`POINT` data type arguments:

```sql
SELECT CALC_DISTANCE(geo1, bar) AS georesult FROM Foo;
```

Using `CALC_DISTANCE` with a NULL value:

```sql
SELECT CALC_DISTANCE(geo1, NULL) AS georesult FROM Foo;
```