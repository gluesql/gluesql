# BYTEA

The `BYTEA` data type in SQL is used to store binary data, such as images, audio files, or any other type of data that needs to be stored in its raw form. In GlueSQL, the BYTEA data type is represented as a sequence of bytes.

Here's an example of how to create a table, insert data, and query data using the `BYTEA` data type:

## Creating a table with a BYTEA column

To create a table with a BYTEA column, use the following SQL syntax:

```sql
CREATE TABLE binary_data (data BYTEA);
```

## Inserting data into the BYTEA column

To insert data into the BYTEA column, provide the binary data in hexadecimal format using the `X` prefix:

```sql
INSERT INTO binary_data (data) VALUES
    (X'123456'),
    (X'ab0123'),
    (X'936DA0');
```

Please note that the hexadecimal string must have an even number of characters, or an error will be thrown.

## Querying data from the BYTEA column

To query data from the BYTEA column, use standard SQL syntax:

```sql
SELECT data FROM binary_data;
```

This query will return the following result:

```
data
----------------
123456
ab0123
936DA0
```

## Error handling

When inserting data into the BYTEA column, you may encounter errors due to incompatible data types or incorrectly formatted hexadecimal strings. For example, inserting a regular integer or an odd-length hexadecimal string will result in an error:

```sql
INSERT INTO binary_data (data) VALUES (0);
-- Error: Incompatible literal for data type BYTEA

INSERT INTO binary_data (data) VALUES (X'123');
-- Error: Failed to decode hexadecimal string
```

## Conclusion

The `BYTEA` data type is essential for storing binary data in SQL databases. By understanding the basics of the BYTEA data type and its use cases, you can effectively use it in your database designs and operations, ensuring that your applications can manage binary data efficiently and securely.