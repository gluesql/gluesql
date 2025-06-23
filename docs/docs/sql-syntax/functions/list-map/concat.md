# CONCAT

The `CONCAT` function is used to concatenate two or more list values together.

## Syntax

```sql
CONCAT(list_value1, list_value2, ...)
```

**Parameters:**

- `list_value1`, `list_value2`, ...: List values that will be concatenated.

## Examples

### Example: CONCAT two lists

```sql
SELECT CONCAT(
  CAST('[1, 2, 3]' AS LIST),
  CAST('["one", "two", "three"]' AS LIST)
) AS myconcat;
```

**Result:**

| myconcat                            |
|-------------------------------------|
| [1, 2, 3, "one", "two", "three"]    |

