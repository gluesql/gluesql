# CHR

The CHR function in SQL returns the character represented by the specified ASCII value.

## Syntax

The syntax for the CHR function in SQL is:

```sql
CHR ( ascii_value )
```

## Parameters

- `ascii_value`: This is the ASCII value for which the character should be returned. It should be an integer value between 0 and 255.

## Examples

Let's consider a few examples to understand how to use the CHR function.

To get the character for an ASCII value:

```sql
VALUES(CHR(70));
```

This will return `'F'`, which is the character for the ASCII value 70.

Please note that the CHR function expects an integer value between 0 and 255. If a value outside this range is passed, it will throw an error. For instance:

```sql
VALUES(CHR(7070));
```

This will throw an error because 7070 is not a valid ASCII value.

You can also use the CHR function in a SELECT statement. Consider the following table named 'Chr':

| id  | num |
| --- | --- |
| 1   | 70  |

```sql
CREATE TABLE Chr (
    id INTEGER,
    num INTEGER
);
INSERT INTO Chr VALUES (1, 70);
```

You can select the character for the 'num' column:

```sql
SELECT CHR(num) AS chr FROM Chr;
```

This will return `'F'`, which is the character for the ASCII value 70.

The CHR function can also take an integer value directly:

```sql
SELECT CHR(65) AS chr FROM Chr;
```

This will return `'A'`, which is the character for the ASCII value 65.

If a non-integer value is passed to the function, it will throw an error. For instance:

```sql
SELECT CHR('ukjhg') AS chr FROM Chr;
```

This will throw an error because 'ukjhg' is not an integer value.

Remember, the CHR function expects an integer value between 0 and 255. If the column value is outside this range, it will throw an error:

```sql
INSERT INTO Chr VALUES (1, 4345);
SELECT CHR(num) AS chr FROM Chr;
```

This will throw an error because 4345 is not a valid ASCII value.