# SQRT

The `SQRT` function is used to calculate the square root of a number. It takes one argument, which must be of the FLOAT type. The result will also be of the FLOAT type.

## Syntax

```sql
SQRT(number)
```

## Examples

1. Using the `SQRT` function:

```sql
SELECT SQRT(2.0) as sqrt_1;
-- Result: 1.4142135623730951
```

2. Using the `SQRT` function with a decimal:

```sql
SELECT SQRT(0.07) as sqrt_2;
-- Result: 0.2645751311064591
```

3. Using the `SQRT` function with an integer:

```sql
SELECT SQRT(32) as sqrt_with_int;
-- Result: 5.656854249492381
```

4. Using the `SQRT` function with zero:

```sql
SELECT SQRT(0) as sqrt_with_zero;
-- Result: 0.0
```

5. Using the `SQRT` function with NULL:

```sql
SELECT SQRT(NULL) AS sqrt;
-- Result: NULL
```

## Error Cases

1. The `SQRT` function requires the argument to be of FLOAT type:

```sql
SELECT SQRT('string') AS sqrt;
-- Error: SqrtOnNonNumeric("string")
```