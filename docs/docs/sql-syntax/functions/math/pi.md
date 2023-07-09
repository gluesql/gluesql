# PI

The `PI` function is used to retrieve the mathematical constant π (pi), which is approximately 3.141592653589793. The function takes no arguments.

## Syntax

```sql
PI()
```

## Examples

Let's consider a table named `SingleItem` with the following schema:

```sql
CREATE TABLE SingleItem (id FLOAT);
```

Insert a row into the `SingleItem` table:

```sql
INSERT INTO SingleItem VALUES (0);
```

### Example 1: Using PI function

```sql
SELECT PI() as pi FROM SingleItem;
```

Result:

```
        pi
----------------
3.141592653589793
```

## Errors

The `PI` function expects no arguments. Providing any arguments will result in an error.

### Example 2: Using PI with an argument

```sql
SELECT PI(0) as pi FROM SingleItem;
```

Error: Function expects 0 arguments, but 1 was provided.