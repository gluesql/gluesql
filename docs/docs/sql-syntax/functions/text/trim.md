# TRIM

The `TRIM` function in SQL is used to remove leading, trailing, or both leading and trailing unwanted characters (often whitespace) from a string.

## Syntax

```sql
TRIM([LEADING | TRAILING | BOTH] [removal_string] FROM target_string)
```

If `LEADING`, `TRAILING`, or `BOTH` is not specified, `TRIM` function will remove both leading and trailing spaces.

## Examples

Here we are creating a table named `Item` with a default value for the `name` column. The default value is obtained by concatenating two strings. The first string is the result of trimming leading 'a' from 'aabc' and the second string is the result of trimming spaces from '   good  '.

```sql
CREATE TABLE Item (
    name TEXT DEFAULT TRIM(LEADING 'a' FROM 'aabc') || TRIM('   good  ')
)
```

We insert some data into the `Item` table:

```sql
INSERT INTO Item VALUES
    ('      Left blank'),
    ('Right blank     '),
    ('     Blank!     '),
    ('Not Blank');
```

The `TRIM` function is used in a `SELECT` statement to remove leading and trailing spaces from the `name` column in the `Item` table:

```sql
SELECT TRIM(name) FROM Item;
```

The `TRIM` function can also be used with `NULL` values. If the value is `NULL`, the `TRIM` function will return `NULL`.

```sql
CREATE TABLE NullName (name TEXT NULL);
INSERT INTO NullName VALUES (NULL);
SELECT TRIM(name) AS test FROM NullName;
```

You can also specify a specific character to remove from the string. The following example removes 'xyz' from the string:

```sql
CREATE TABLE Test (name TEXT);
INSERT INTO Test VALUES
        ('     blank     '), 
        ('xxxyzblankxyzxx'), 
        ('xxxyzblank     '),
        ('     blankxyzxx'),
        ('  xyzblankxyzxx'),
        ('xxxyzblankxyz  ');
SELECT TRIM(BOTH 'xyz' FROM name) FROM Test;
```

The `LEADING` and `TRAILING` keywords can be used to remove characters from the beginning or the end of the string, respectively:

```sql
SELECT TRIM(LEADING 'xyz' FROM name) FROM Test;
SELECT TRIM(TRAILING 'xyz' FROM name) FROM Test;
```

You can also nest `TRIM` functions:

```sql
SELECT TRIM(BOTH TRIM(BOTH ' potato ')) AS Case1;
```

The `TRIM` function requires string values. If you try to use it with a non-string value, an error will occur:

```sql
SELECT TRIM('1' FROM 1) AS test FROM Test;
```