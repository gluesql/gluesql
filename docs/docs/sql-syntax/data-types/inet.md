# INET

The `INET` data type in SQL is used to store IPv4 and IPv6 addresses. These addresses can be compared, filtered, and sorted using standard SQL operations.

Here's an example of how to create a table, insert data, and query data using the `INET` data type:

## Creating a table with an INET column

To create a table with an INET column, use the following SQL syntax:

```sql
CREATE TABLE computer (ip INET);
```

## Inserting data into the INET column

To insert data into the INET column, provide the IP addresses as strings or integers:

```sql
INSERT INTO computer VALUES
    ('::1'),
    ('127.0.0.1'),
    ('0.0.0.0'),
    (4294967295),
    (9876543210);
```

## Querying data from the INET column

To query data from the INET column, use standard SQL syntax:

```sql
SELECT * FROM computer;
```

This query will return the following result:

```
ip
-----------------
::1
127.0.0.1
0.0.0.0
255.255.255.255
::2:4cb0:16ea
```

## Filtering data using the INET column

You can filter data using the INET column with standard SQL operators:

```sql
SELECT * FROM computer WHERE ip > '127.0.0.1';
```

This query will return the following result:

```
ip
-----------------
::1
255.255.255.255
::2:4cb0:16ea
```

## Querying for specific IP addresses

To query for specific IP addresses, use the following syntax:

```sql
SELECT * FROM computer WHERE ip = '127.0.0.1';
```

This query will return the following result:

```
ip
---------
127.0.0.1
```