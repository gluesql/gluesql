# CAST

The `CAST` function is used to convert a value from one data type to another. It is commonly used when you need to change the data type of a value or a column to perform a specific operation, such as arithmetic or string concatenation.

## Syntax

```sql
CAST(expression AS data_type)
```

- `expression`: The value or column you want to convert.
- `data_type`: The target data type to which you want to convert the expression.

## Examples

### Converting a value to a different data type

```sql
SELECT CAST('TRUE' AS BOOLEAN) AS cast;
```

In this example, the `CAST` function is used to convert the string `'TRUE'` to a boolean value.

### Converting a column to a different data type

Suppose you have a table called `employees` with the following structure:

```sql
CREATE TABLE employees (id INT, name TEXT, salary TEXT);
```

To calculate the total salary of all employees, you can use the `CAST` function to convert the `salary` column to a `DECIMAL` data type:

```sql
SELECT SUM(CAST(salary AS DECIMAL)) AS total_salary FROM employees;
```

### Handling NULL values

The `CAST` function can handle NULL values as well. If the expression is NULL, the result will be NULL:

```sql
SELECT CAST(NULL AS INTEGER) AS cast;
```

This query will return a NULL value.

### Converting a value to a DATE or TIME data type

The `CAST` function can also be used to convert strings to DATE or TIME data types:

```sql
SELECT CAST('2023-05-04' AS DATE) AS cast_date;
SELECT CAST('14:30:00' AS TIME) AS cast_time;
```

These queries will return a date and time value, respectively.

## Limitations and Errors

Some conversions may be impossible or result in an error. For example, trying to convert a non-numeric string to an integer will result in an error:

```sql
SELECT CAST('foo' AS INTEGER) AS cast;
```

This query will produce an error because the string `'foo'` cannot be converted to an integer.