# CALC_DISTANCE

The `CALC_DISTANCE` function is used to calculate the Euclidean distance between two `Point` type geographical coordinates.

## Syntax

```sql
CALC_DISTANCE(point1, point2)
```

**Parameters:**

- `point1`: The first geographical coordinate of type `Point`.
- `point2`: The second geographical coordinate of type `Point`.

## Examples

Consider the following table `Foo`:

```sql
CREATE TABLE Foo (
    geo1 Point,
    geo2 Point,
    bar Float
);
```

With the following data:

```sql
INSERT INTO Foo VALUES (POINT(0.3134, 3.156), POINT(1.415, 3.231), 3);
```

### Example 1: Calculate the distance between two points

```sql
SELECT CALC_DISTANCE(geo1, geo2) AS georesult FROM Foo;
```

**Result:**

| georesult       |
|-----------------|
| 1.104150152832485|

## Errors

1. If the number of arguments is not 2, a `FunctionArgsLengthNotMatching` error will be thrown.
2. If any of the arguments are not of type `Point`, a `FunctionRequiresPointValue` error will be thrown.
3. If any of the arguments are `NULL`, the result will be `NULL`.
