# POWER

The `POWER` function is used to raise a number to the power of another number. It takes two arguments, the base and the exponent, both of which must be of the FLOAT type. The result will also be of the FLOAT type.

## Syntax

```sql
POWER(base, exponent)
```

## Examples

1. Using the `POWER` function:

```sql
SELECT POWER(2.0, 4) as power_1;
-- Result: 16.0
```

2. Using the `POWER` function with a decimal:

```sql
SELECT POWER(0.07, 3) as power_2;
-- Result: 0.000343
```

3. Using the `POWER` function with zero:

```sql
SELECT POWER(0, 4) as power_with_zero;
-- Result: 0.0

SELECT POWER(3, 0) as power_to_zero;
-- Result: 1.0
```

## Error Cases

1. The `POWER` function requires both arguments to be of FLOAT type:

```sql
SELECT POWER('string', 'string') AS power;
-- Error: FunctionRequiresFloatValue("POWER")
```

2. The `POWER` function requires the base to be of FLOAT type:

```sql
SELECT POWER('string', 2.0) AS power;
-- Error: FunctionRequiresFloatValue("POWER")
```

3. The `POWER` function requires the exponent to be of FLOAT type:

```sql
SELECT POWER(2.0, 'string') AS power;
-- Error: FunctionRequiresFloatValue("POWER")
```