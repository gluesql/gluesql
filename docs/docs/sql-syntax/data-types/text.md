---
sidebar_position: 4
---

# TEXT

The `TEXT` data type in SQL is used to store variable-length character strings. In GlueSQL, the TEXT data type is the only supported string data type, providing the ability to store and manage strings of varying lengths.

Here's an example of how to create a table, insert data, and query data using the `TEXT` data type:

## Creating a table with a TEXT column

To create a table with a TEXT column, use the following SQL syntax:

```sql
CREATE TABLE users (username TEXT, email TEXT);
```

## Inserting data into the TEXT column

To insert data into the TEXT column, provide the string values:

```sql
INSERT INTO users (username, email) VALUES
    ('user1', 'user1@example.com'),
    ('user2', 'user2@example.com'),
    ('user3', 'user3@example.com');
```

## Querying data from the TEXT column

To query data from the TEXT column, use standard SQL syntax:

```sql
SELECT username, email FROM users;
```

This query will return the following result:

```
username | email
---------|-------------------
user1    | user1@example.com
user2    | user2@example.com
user3    | user3@example.com
```

## Conclusion

The `TEXT` data type is a versatile and essential data type for handling and storing character strings in SQL databases. By understanding the basics of the TEXT data type and its use cases, you can effectively use it in your database designs and operations, ensuring that your applications can manage a wide range of textual data with ease.