---
sidebar_position: 5
---

# DECIMAL

The `DECIMAL` data type in SQL is used to store exact numeric values, making it suitable for financial calculations and other operations requiring a high level of precision without round-off errors. In GlueSQL, the DECIMAL data type is implemented using a pure Rust library, providing a 96-bit integer number, a scaling factor for specifying the decimal fraction, and a 1-bit sign.

Here's an example of how to create a table, insert data, and query data using the `DECIMAL` data type:

## Creating a table with a DECIMAL column

To create a table with a DECIMAL column, use the following SQL syntax:

```sql
CREATE TABLE financial_data (description TEXT, value DECIMAL);
```

## Inserting data into the DECIMAL column

To insert data into the DECIMAL column, provide the exact numeric values:

```sql
INSERT INTO financial_data (description, value) VALUES
    ('Revenue', 15000.25),
    ('Expense', 12000.75),
    ('Profit', 2999.50);
```

## Querying data from the DECIMAL column

To query data from the DECIMAL column, use standard SQL syntax:

```sql
SELECT description, value FROM financial_data;
```

This query will return the following result:

```
description | value
------------|---------
Revenue     | 15000.25
Expense     | 12000.75
Profit      |  2999.50
```

## Truncating trailing zeros

In GlueSQL's DECIMAL implementation, trailing zeros are preserved in the binary representation and may be exposed when converting the value to a string. To truncate trailing zeros, you can use the `normalize` or `round_dp` functions in Rust.

## Conclusion

The `DECIMAL` data type is crucial for handling precise numeric values in SQL databases, especially in financial calculations and other applications requiring high accuracy without round-off errors. By understanding the basics of the DECIMAL data type and its use cases, you can effectively use it in your database designs and operations, ensuring that your applications can manage exact numeric values with precision.