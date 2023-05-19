# ABS

The `ABS` function is used to calculate the absolute value of a number. It takes a single numeric argument and returns the absolute value of that number. The argument can be an integer, decimal, or float value.

## Syntax

```sql
ABS(value)
```

- `value`: A numeric expression for which the absolute value is to be calculated.

## Examples

Let's consider a table named `SingleItem` with the following schema:

```sql
CREATE TABLE SingleItem (id INTEGER, int8 INT8, dec DECIMAL);
```

Insert a row into the `SingleItem` table:

```sql
INSERT INTO SingleItem VALUES (0, -1, -2);
```

### Example 1: Using ABS with integer values

```sql
SELECT ABS(1) AS ABS1, 
       ABS(-1) AS ABS2, 
       ABS(+1) AS ABS3 
FROM SingleItem;
```

Result:

```
 ABS1 | ABS2 | ABS3 
------+------+------
    1 |    1 |    1 
```

### Example 2: Using ABS with float values

```sql
SELECT ABS(1.0) AS ABS1, 
       ABS(-1.0) AS ABS2, 
       ABS(+1.0) AS ABS3 
FROM SingleItem;
```

Result:

```
 ABS1 | ABS2 | ABS3 
------+------+------
  1.0 |  1.0 |  1.0 
```

### Example 3: Using ABS with table columns

```sql
SELECT ABS(id) AS ABS1, 
       ABS(int8) AS ABS2, 
       ABS(dec) AS ABS3 
FROM SingleItem;
```

Result:

```
 ABS1 | ABS2 | ABS3 
------+------+------
    0 |    1 |    2 
```

### Example 4: Using ABS with NULL values

```sql
SELECT ABS(NULL) AS ABS FROM SingleItem;
```

Result:

```
  ABS  
-------
 (null)
```

## Errors

The `ABS` function requires a numeric value as its argument. Using non-numeric values or more than one argument will result in an error.

### Example 5: Using ABS with non-numeric values

```sql
SELECT ABS('string') AS ABS FROM SingleItem;
```

Error: Function requires a numeric value.

### Example 6: Using ABS with multiple arguments

```sql
SELECT ABS('string', 'string2') AS ABS FROM SingleItem;
```

Error: Function expects 1 argument, but 2 were provided.