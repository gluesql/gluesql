# INITCAP

The `INITCAP` function in SQL is used to capitalize the first letter of each word in a string and convert the rest of the characters to lowercase.

## Syntax

The syntax for the `INITCAP` function in SQL is:

```sql
INITCAP( string )
```

## Parameters

- `string`: The input string on which the capitalization will be applied.

## Examples

Let's consider a few examples to understand how to use the `INITCAP` function.

Create a table named `Item` with a column `name`:

```sql
CREATE TABLE Item (
    name TEXT DEFAULT 'abcd'
);
```

Insert some data into the `Item` table:

```sql
INSERT INTO Item VALUES
('h/i jk'),
(NULL),
('H/I JK');
```

Select rows where the `INITCAP(name)` is equal to 'H/I Jk':

```sql
SELECT name FROM Item WHERE INITCAP(name) = 'H/I Jk';
```

This will return the rows with 'h/i jk' and 'H/I JK', as both have the same result after applying the `INITCAP` function.

Apply the `INITCAP` function to the `name` column and return the result:

```sql
SELECT INITCAP(name) FROM Item;
```

This will return 'H/I Jk', NULL, and 'H/I Jk' for the three rows, respectively.

The `INITCAP` function expects a string value as the input. If a non-string value is passed as the input, it will throw an error:

```sql
SELECT INITCAP(1) FROM Item;
```

This will throw an error because the `INITCAP` function expects a string value as the input.

The `INITCAP` function expects a single argument. If no arguments are provided, it will throw an error:

```sql
SELECT INITCAP() FROM Item;
```

This will throw an error because the `INITCAP` function expects a single argument.

The `INITCAP` function does not support named arguments. If a named argument is provided, it will throw an error:

```sql
SELECT INITCAP(a => 2) FROM Item;
```

This will throw an error because the `INITCAP` function does not support named arguments.
