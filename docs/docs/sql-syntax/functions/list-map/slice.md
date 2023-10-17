# SLICE

The `SLICE` statement is a function in GlueSQL that allows you to retrieve a subsection of a list. It is analogous to slicing operations in many programming languages.

## Syntax

```sql
SELECT SLICE(column_name, start_index, length) AS alias_name FROM table_name;
```

- `column_name`: Name of the column containing the list.
- `start_index`: The starting index from where the slice should begin. This value can be negative.
- `length`: The number of elements to be included in the slice.

## Examples

Consider the following table `Test`:

```sql
CREATE TABLE Test (
    list LIST
);
```

With the following data:

```sql
INSERT INTO Test VALUES
('[1,2,3,4]');
```

### 1. Basic Slicing
Retrieve the first 2 elements from a list.

```sql
SELECT SLICE(list, 0, 2) AS value FROM Test;
```

Result:
```
[1, 2]
```

### 2. Slicing Beyond List Length
If the combined start index and length exceed the list size, `SLICE` will return all possible elements without error.

```sql
SELECT SLICE(list, 2, 5) AS value FROM Test;
```

Result:
```
[3, 4]
```

### 3. Start Index Beyond List Length
If the start index alone exceeds the list size, `SLICE` will return an empty list.

```sql
SELECT SLICE(list, 100, 5) AS value FROM Test;
```

Result:
```
[]
```

### 4. Using Negative Start Index
A negative start index counts from the end of the list.

```sql
SELECT SLICE(list, -1, 1) AS value FROM Test;
```

Result:
```
[4]
```

Another example of a negative start index.

```sql
SELECT SLICE(list, -2, 4) AS value FROM Test;
```

Result:
```
[3, 4]
```

If the absolute value of the negative start index exceeds the list length, it is treated as index 0.

```sql
SELECT SLICE(list, -234, 4) AS value FROM Test;
```

Result:
```
[1, 2, 3, 4]
```

## Errors

- Using a non-list value for slicing will result in an error: `ListTypeRequired`.
- Using a non-integer value for the start index or length will result in an error: `FunctionRequiresIntegerValue("SLICE")`.