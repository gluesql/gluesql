# HEX

`HEX` converts an `INT` or string value to uppercase hexadecimal text.

## Syntax

```sql
HEX(value)
```

## Parameters

- `value` - An `INT` (`INTEGER`) or string value to convert. Integer literals such as `228` are `INT` values.

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

Other integer types, such as `INT32` or `UINT8`, are not accepted and return an error. Cast them to `INT` first, for example `HEX(CAST(value AS INT))`.

Negative `INT` values are converted as 64-bit two's complement, so `HEX(-123)` returns `FFFFFFFFFFFFFF85`.
