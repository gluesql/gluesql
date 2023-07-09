# PREPEND

The `PREPEND` function in SQL is used to prepend an element to a list.

## Syntax

```sql
PREPEND(list, element)
```

- `list`: The list to which you want to prepend the element.
- `element`: The element that you want to prepend to the list.

## Examples

First, create a table named `Prepend` with columns for the list, an integer element, and a text element:

```sql
CREATE TABLE Prepend (
    id INTEGER,
    items LIST,
    element INTEGER,
    element2 TEXT
);
```

Insert some data into the `Prepend` table:

```sql
INSERT INTO Prepend VALUES
(1, '[1, 2, 3]', 0, 'Foo');
```

Use the `PREPEND` function to prepend the integer element to the list:

```sql
SELECT PREPEND(items, element) AS myprepend FROM Prepend;
```

Use the `PREPEND` function to prepend the text element to the list:

```sql
SELECT PREPEND(items, element2) AS myprepend FROM Prepend;
```

The `PREPEND` function requires a list as the first parameter. If you try to use it with a non-list value, an error will occur:

```sql
SELECT PREPEND(element, element2) AS myprepend FROM Prepend;
```

You can also use the `PREPEND` function when inserting data into a table. First, create a table named `Foo` with a column for the list:

```sql
CREATE TABLE Foo (
    elements LIST
);
```

Then, insert data into the `Foo` table using the `PREPEND` function:

```sql
INSERT INTO Foo VALUES (PREPEND(CAST('[1, 2, 3]' AS LIST), 0));
```

Finally, retrieve the list from the `Foo` table:

```sql
SELECT elements AS myprepend FROM Foo;
```
