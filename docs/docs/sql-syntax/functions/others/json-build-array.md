# JSON_BUILD_ARRAY

The `JSON_BUILD_ARRAY` function builds an array from a variable number of expressions. It accepts values of different types and preserves `NULL` values as `null` elements.

## Syntax

```sql
JSON_BUILD_ARRAY(expr1, expr2, ...)
```

The function accepts zero or more expressions.

## Examples

```sql
SELECT JSON_BUILD_ARRAY(1, 'GlueSQL', TRUE, NULL) AS result;
```

This returns `[1,"GlueSQL",true,null]` as a `LIST` value.

Nested `LIST` and `MAP` values are preserved:

```sql
SELECT JSON_BUILD_ARRAY(
    CAST('[1, 2]' AS LIST),
    CAST('{"name": "GlueSQL"}' AS MAP)
) AS result;
```

This returns `[[1,2],{"name":"GlueSQL"}]` as a `LIST` value.

## PostgreSQL compatibility

`JSON_BUILD_ARRAY` follows PostgreSQL's variadic, heterogeneously typed array-building behavior. PostgreSQL returns a `json` value, while GlueSQL represents the result as a `LIST` value. There is no separate `JSON` or `JSONB` type in GlueSQL; nested `LIST` and `MAP` values are converted to JSON-compatible output when serialized.
