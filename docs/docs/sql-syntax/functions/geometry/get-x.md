# GET_X

The `GET_X` function returns the x-coordinate of a given `POINT` data type. It takes one `POINT` data type argument and returns a `FLOAT` value representing the x-coordinate.

## Syntax

```sql
GET_X(point)
```

## Examples

Create a table with a default value for the `id` column using `GET_X`:

```sql
CREATE TABLE SingleItem (id FLOAT DEFAULT GET_X(POINT(0.3134, 0.156)));
```

Get the x-coordinate of a point value:

```sql
SELECT GET_X(CAST('POINT(0.1 -0.2)' AS POINT)) AS ptx;
```

```sql
SELECT GET_X(POINT(0.1, -0.2)) AS ptx;
```

Create a table with a `POINT` data type column:

```sql
CREATE TABLE POINT (point_field POINT);
```

Insert a record with a point value:

```sql
INSERT INTO POINT VALUES (POINT(0.3134, 0.156));
```

Select the x-coordinate of the `point_field` column:

```sql
SELECT GET_X(point_field) AS point_field FROM POINT;
```

## Invalid Use Case

Trying to use `GET_X` with a non-`POINT` data type argument:

```sql
SELECT GET_X('cheese') AS ptx;
```
