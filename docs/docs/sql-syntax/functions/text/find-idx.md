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

Find the position of 'rg' in 'pork':

```sql
SELECT FIND_IDX('pork', 'rg') AS test;
```

This will return 0, as 'rg' is not found in 'pork'.

Find the position of 'rg' in 'burger':

```sql
SELECT FIND_IDX('burger', 'rg') AS test;
```

This will return 3, as the first occurrence of 'rg' in 'burger' is at position 3.

Find the position of 'r' in 'pork', starting from position 4:

```sql
SELECT FIND_IDX('pork', 'r', 4) AS test;
```

This will return 0, as 'r' is not found in 'pork' after position 4.

Find the position of 'r' in 'burger', starting from position 4:

```sql
SELECT FIND_IDX('burger', 'r', 4) AS test;
```

This will return 6, as the first occurrence of 'r' in 'burger' after position 4 is at position 6.

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