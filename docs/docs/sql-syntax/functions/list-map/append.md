# APPEND

The `APPEND` function in SQL is used to append an element to a list.

## Syntax

```sql
APPEND(list, element)
```

- `list`: The list to which you want to append the element.
- `element`: The element that you want to append to the list.

## Examples

First, create a table named `Append` with columns for the list, an integer element, and a text element:

```sql
CREATE TABLE Append (
    id INTEGER,
    items LIST,
    element INTEGER,
    element2 TEXT
);
```

Insert some data into the `Append` table:

```sql
INSERT INTO Append VALUES
(1, '[1, 2, 3]', 4, 'Foo');
```

Use the `APPEND` function to append the integer element to the list:

```sql
SELECT APPEND(items, element) AS myappend FROM Append;
```

Use the `APPEND` function to append the text element to the list:

```sql
SELECT APPEND(items, element2) AS myappend FROM Append;
```

The `APPEND` function requires a list as the first parameter. If you try to use it with a non-list value, an error will occur:

```sql
SELECT APPEND(element, element2) AS myappend FROM Append;
```

You can also use the `APPEND` function when inserting data into a table. First, create a table named `Foo` with a column for the list:

```sql
CREATE TABLE Foo (
    elements LIST
);
```

Then, insert data into the `Foo` table using the `APPEND` function:

```sql
INSERT INTO Foo VALUES (APPEND(CAST('[1, 2, 3]' AS LIST), 4));
```

Finally, retrieve the list from the `Foo` table:

```sql
SELECT elements AS myappend FROM Foo;
```