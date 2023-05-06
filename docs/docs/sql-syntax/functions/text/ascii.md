# ASCII

The ASCII function in SQL returns the ASCII value for the first character of the specified string. 

## Syntax

The syntax for the ASCII function in SQL is:

```sql
ASCII ( single_character_text )
```

## Parameters

- `single_character_text`: This is the string that the ASCII value should be returned for. It should be a single character string. 

## Examples

Let's consider a few examples to understand how to use the ASCII function.

To get the ASCII value of a character:

```sql
VALUES(ASCII('A'));
```

This will return `65`, which is the ASCII value for 'A'.

Please note that the ASCII function expects a single character value. If a string with more than one character is passed, it will throw an error. For instance:

```sql
VALUES(ASCII('AB'));
```

This will throw an error because 'AB' contains more than one character.

You can also use the ASCII function in a SELECT statement. Consider the following table named 'Ascii':

| id  | text |
| --- | ---- |
| 1   | 'F'  |

```sql
CREATE TABLE Ascii (
    id INTEGER,
    text TEXT
);
INSERT INTO Ascii VALUES (1, 'F');
```

You can select the ASCII value of the 'text' column:

```sql
SELECT ASCII(text) AS ascii FROM Ascii;
```

This will return `70`, which is the ASCII value for 'F'.

The ASCII function can also take a string directly:

```sql
SELECT ASCII('a') AS ascii FROM Ascii;
```

This will return `97`, which is the ASCII value for 'a'.

If a non-ASCII character is passed to the function, it will throw an error. For instance:

```sql
SELECT ASCII('ㄱ') AS ascii FROM Ascii;
```

This will throw an error because 'ㄱ' is not an ASCII character.

If no argument is passed to the ASCII function, it will also throw an error:

```sql
SELECT ASCII() AS ascii FROM Ascii;
```

This will throw an error because the ASCII function expects one argument.

Remember, the ASCII function expects a single character. If the column value contains more than one character, it will throw an error:

```sql
INSERT INTO Ascii VALUES (1, 'Foo');
SELECT ASCII(text) AS ascii FROM Ascii;
```

This will throw an error because 'Foo' contains more than one character.