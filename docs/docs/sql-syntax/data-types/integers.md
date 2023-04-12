---
sidebar_position: 2
---

# Integer Types

GlueSQL supports the following integer data types:
- `INT8`: 8-bit signed integer
- `INT16`: 16-bit signed integer
- `INT32`: 32-bit signed integer
- `INT` or `INTEGER`: 64-bit signed integer (default)
- `INT128`: 128-bit signed integer
- `UINT8`: 8-bit unsigned integer
- `UINT16`: 16-bit unsigned integer
- `UINT32`: 32-bit unsigned integer
- `UINT64`: 64-bit unsigned integer
- `UINT128`: 128-bit unsigned integer

For general purposes, you can use `INTEGER` to specify a 64-bit signed integer.

Here's an example of how to create a table with integer data types:

```
CREATE TABLE Item (
  field_one INTEGER,
  field_two INTEGER
);
```

You can insert data into the `Item` table as follows:

```
INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);
```

You can perform arithmetic operations such as addition, subtraction, multiplication, division, and modulo on integer columns. Note that if you perform arithmetic operations on columns with different integer types, GlueSQL will automatically convert the types of the operands to match the type of the left-hand operand. For example, if you perform `UINT8 + INT64`, GlueSQL will convert the `INT64` operand to `UINT8` and then perform the addition.

Integer types are an important part of SQL, and you can use them to store data ranging from small whole numbers to large integers. By understanding how to use integer types in your database, you can write efficient and effective SQL queries that work with a wide range of data.