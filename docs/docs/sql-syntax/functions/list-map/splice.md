# SQL Function - "SPLICE"

The "SPLICE" function in GlueSQL is used to modify elements in a list. It allows you to remove or replace elements in a list. The syntax for the "SPLICE" function is as follows:

```sql
SPLICE(list_expression, start, end [, replace_elements])
```

- `list_expression`: The list you want to modify.
- `start`: The position at which you want to start the modification.
- `end`: The exclusive end position for the modification.
- `replace_elements` (optional): A list of elements to insert in place of the removed elements.

## Example

Let's consider a table named "ListTable" with the following data:

```sql
CREATE TABLE ListTable (
    id INTEGER,
    items LIST
);

INSERT INTO ListTable VALUES
    (1, '[1, 2, 3]'),
    (2, '["1", "2", "3"]'),
    (3, '["1", 2, 3]');
```

Now, we can use the "SPLICE" function to modify the list in various ways:

1. Remove elements from a list:

```sql
-- Remove elements at positions 1 to 3 from the list '[1, 2, 3]'
SELECT SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 4) AS result;
-- Output: '[1, 4, 5]'
```

2. Replace elements in a list:

```sql
-- Replace elements at positions 1 to 3 with '[100, 99]' in the list '[1, 2, 3, 4, 5]'
SELECT SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 4, CAST('[100, 99]' AS List)) AS result;
-- Output: '[1, 100, 99, 5]'
```

3. 'start' is processed so that it is not less than 0 and 'end' is not greater than the length of the list.

```sql
SELECT SPLICE(CAST('[1, 2, 3]' AS List), -1, 2, CAST('[100, 99]' AS List)) AS result;
-- Output: '[100, 99, 3]'
```

## Error

If you use the "SPLICE" function with invalid inputs, it will result in an error. For example:

```sql
-- Using SPLICE on a non-list value will result in an error.
SELECT SPLICE(3, 1, 2) AS result;
-- EvaluateError::ListTypeRequired
```