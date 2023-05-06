# RAND

The `RAND` function is used to generate a random float value between 0 (inclusive) and 1 (exclusive). The function takes an optional seed value, which must be of the FLOAT type. If a seed value is provided, the random number generator will be initialized with that seed, producing a deterministic sequence of random numbers.

## Syntax

```sql
RAND([seed])
```

## Examples

1. Using the `RAND` function without a seed:

```sql
SELECT RAND() AS rand;
-- Result: A random float between 0 and 1
```

2. Using the `RAND` function with a seed:

```sql
SELECT RAND(123) AS rand1, RAND(789.0) AS rand2;
-- Result: 0.17325464426155657, 0.9635218234007941
```

3. Using the `RAND` function with NULL:

```sql
SELECT RAND(NULL) AS rand;
-- Result: NULL
```

## Error Cases

1. The `RAND` function requires the argument to be of FLOAT type, if provided:

```sql
SELECT RAND('string') AS rand;
-- Error: FunctionRequiresFloatValue("RAND")

SELECT RAND(TRUE) AS rand;
-- Error: FunctionRequiresFloatValue("RAND")

SELECT RAND(FALSE) AS rand;
-- Error: FunctionRequiresFloatValue("RAND")
```

2. The `RAND` function takes at most one argument:

```sql
SELECT RAND('string', 'string2') AS rand;
-- Error: FunctionArgsLengthNotWithinRange("RAND", 0, 1, 2)
```