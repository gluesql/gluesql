# SIGN

The `SIGN` function is used to determine the sign of a number. It takes one argument, which must be of the FLOAT type. The result will be of the INTEGER type and can be -1, 0, or 1.

## Syntax

```sql
SIGN(number)
```

## Examples

1. Using the `SIGN` function with integers:

```sql
SELECT SIGN(2) AS SIGN1, 
       SIGN(-2) AS SIGN2, 
       SIGN(+2) AS SIGN3;
-- Result: 1, -1, 1
```

2. Using the `SIGN` function with floats:

```sql
SELECT SIGN(2.0) AS SIGN1, 
       SIGN(-2.0) AS SIGN2, 
       SIGN(+2.0) AS SIGN3;
-- Result: 1, -1, 1
```

3. Using the `SIGN` function with zero:

```sql
SELECT SIGN(0.0) AS SIGN1, 
       SIGN(-0.0) AS SIGN2, 
       SIGN(+0.0) AS SIGN3;
-- Result: 0, 0, 0
```

4. Using the `SIGN` function with NULL:

```sql
SELECT SIGN(NULL) AS sign;
-- Result: NULL
```

## Error Cases

1. The `SIGN` function requires the argument to be of FLOAT type:

```sql
SELECT SIGN('string') AS SIGN;
-- Error: FunctionRequiresFloatValue("SIGN")

SELECT SIGN(TRUE) AS sign;
-- Error: FunctionRequiresFloatValue("SIGN")

SELECT SIGN(FALSE) AS sign;
-- Error: FunctionRequiresFloatValue("SIGN")
```

2. The `SIGN` function takes exactly one argument:

```sql
SELECT SIGN('string', 'string2') AS SIGN;
-- Error: FunctionArgsLengthNotMatching("SIGN", 1, 2)
```