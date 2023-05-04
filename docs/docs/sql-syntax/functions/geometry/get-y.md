# GET_Y

The `GET_Y` function returns the y-coordinate of a given `POINT` data type. It takes one `POINT` data type argument and returns a `FLOAT` value representing the y-coordinate.

## Syntax

```sql
GET_Y(point)
```

## Examples

Create a table with a default value for the `id` column using `GET_Y`:

```sql
CREATE TABLE SingleItem (id FLOAT DEFAULT GET_Y(POINT(0.3134, 0.156)));
```

Get the y-coordinate of a point value:

```sql
SELECT GET_Y(CAST('POINT(0.1 -0.2)' AS POINT)) AS ptx;
```

```sql
SELECT GET_Y(POINT(0.1, -0.2)) AS ptx;
```

Create a table with a `POINT` data type column:

```sql
CREATE TABLE POINT (point_field POINT);
```

Insert a record with a point value:

```sql
INSERT INTO POINT VALUES (POINT(0.3134, 0.156));
```

Select the y-coordinate of the `point_field` column:

```sql
SELECT GET_Y(point_field) AS point_field FROM POINT;
```

## Invalid Use Case

Trying to use `GET_Y` with a non-`POINT` data type argument:

```sql
SELECT GET_Y('cheese') AS ptx;
```