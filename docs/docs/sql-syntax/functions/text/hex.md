# HEX

`HEX` converts an integer or string value to uppercase hexadecimal text.

## Syntax

```sql
HEX(value)
```

## Parameters

- `value` - An integer or string value to convert.

## Examples

Convert text to hexadecimal:

```sql
SELECT HEX('Hello World');
```

This returns `48656C6C6F20576F726C64`.

Convert an integer to hexadecimal:

```sql
SELECT HEX(228);
```

This returns `E4`.

## Notes

`HEX` requires exactly one argument. If the argument is `NULL`, the result is `NULL`.
