# FIND_IDX

The `FIND_IDX` function in SQL is used to return the position of the first occurrence of a substring in a string, optionally after a specified position.

## Syntax

The syntax for the `FIND_IDX` function in SQL is:

```sql
FIND_IDX ( string, substring, [ start_position ] )
```

## Parameters

- `string`: The string where the search will take place.
- `substring`: The substring to find.
- `start_position` (optional): The position at which to start the search. The first position in the string is 0. If the `start_position` is not specified, the search starts from the beginning of the string.

## Examples

Let's consider a few examples to understand how to use the `FIND_IDX` function.

Find the position of 'rg' in each 'menu' value:

```sql
SELECT FIND_IDX(menu, 'rg') AS test FROM Meal;
```

This will return 0 for 'pork' and 3 for 'burger'.

Find the position of 'r' in each 'menu' value, starting from position 4:

```sql
SELECT FIND_IDX(menu, 'r', 4) AS test FROM Meal;
```

This will return 0 for 'pork' and 6 for 'burger'.

Find the position of an empty string in 'cheese':

```sql
SELECT FIND_IDX('cheese', '') AS test;
```

This will return 0, because the search starts at the first position by default and the empty string is considered to be found at the start of any string.

Find the position of 's' in 'cheese':

```sql
SELECT FIND_IDX('cheese', 's') AS test;
```

This will return 5, as the first occurrence of 's' in 'cheese' is at position 5.

Find the position of 'e' in 'cheese burger', starting from position 5:

```sql
SELECT FIND_IDX('cheese burger', 'e', 5) AS test;
```

This will return 6, because the search starts from position 5 and the next 'e' is at position 6.

Using a NULL value as the substring will return NULL:

```sql
SELECT FIND_IDX('cheese', NULL) AS test;
```

This will return NULL.

The `FIND_IDX` function expects a string value as the substring. If a non-string value is passed as the substring, it will throw an error:

```sql
SELECT FIND_IDX('cheese', 1) AS test;
```

This will throw an error because the `FIND_IDX` function expects a string value as the substring.

The `FIND_IDX` function expects an integer value as the `start_position`. If a non-integer value is passed as the `start_position`, it will throw an error:

```sql
SELECT FIND_IDX('cheese', 's', '5') AS test;
```

This will throw an error because the `FIND_IDX` function expects an integer value as the `start_position`.

The `start_position` must be a non-negative integer. If a negative integer is passed as the `start_position`, it will throw an error:

```sql
SELECT FIND_IDX('cheese', 's', -1) AS test;
```

This will throw an error because the `FIND_IDX` function expects a non-negative integer as the `start_position`.