---
sidebar_position: 3
---

# FLOAT

The `FLOAT` data type in SQL is used to store floating-point numbers. In GlueSQL, the FLOAT data type represents a 64-bit floating-point number, providing the ability to store numbers with decimal values and a wide range of magnitude.

Here's an example of how to create a table, insert data, and query data using the `FLOAT` data type:

## Creating a table with a FLOAT column

To create a table with a FLOAT column, use the following SQL syntax:

```sql
CREATE TABLE product_prices (product_name TEXT, price FLOAT);
```

## Inserting data into the FLOAT column

To insert data into the FLOAT column, provide the floating-point values:

```sql
INSERT INTO product_prices (product_name, price) VALUES
    ('Product A', 19.99),
    ('Product B', 39.49),
    ('Product C', 12.75);
```

## Querying data from the FLOAT column

To query data from the FLOAT column, use standard SQL syntax:

```sql
SELECT product_name, price FROM product_prices;
```

This query will return the following result:

```
product_name | price
-------------|-------
Product A    | 19.99
Product B    | 39.49
Product C    | 12.75
```

## Conclusion

The `FLOAT` data type is essential for handling numeric data with decimal values and various magnitudes. By understanding the basics of the FLOAT data type and its use cases, you can effectively use it in your database designs and operations, ensuring that your applications can handle a wide range of numerical values with precision.