---
sidebar_position: 1
---

# WHERE

In GlueSQL, the `WHERE` clause is used to filter the results of a `SELECT` query based on specific conditions. The `WHERE` clause can be used with various operators and functions to create complex filtering conditions.

Here are some examples based on the provided Rust test code and SQL queries:

## Comparison Operators

You can use comparison operators such as `=`, `<>`, `<`, `>`, `<=`, and `>=` to compare values in the `WHERE` clause.

```sql
SELECT name FROM Boss WHERE id <= 2;
SELECT name FROM Boss WHERE +id <= 2;
```

## BETWEEN Operator

The `BETWEEN` operator allows you to filter results within a specific range.

```sql
SELECT id, name FROM Boss WHERE id BETWEEN 2 AND 4;
SELECT id, name FROM Boss WHERE name BETWEEN 'Doll' AND 'Gehrman';
```

To exclude the specified range, use the `NOT BETWEEN` operator.

```sql
SELECT name FROM Boss WHERE name NOT BETWEEN 'Doll' AND 'Gehrman';
```

## EXISTS and NOT EXISTS

`EXISTS` and `NOT EXISTS` operators are used to filter results based on the existence of records in a subquery.

```sql
SELECT name
FROM Boss
WHERE EXISTS (
    SELECT * FROM Hunter WHERE Hunter.name = Boss.name
);

SELECT name
FROM Boss
WHERE NOT EXISTS (
    SELECT * FROM Hunter WHERE Hunter.name = Boss.name
);
```

## IN Operator

The `IN` operator allows you to filter results based on a list of values or a subquery.

```sql
SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE quantity IN (6, 7, 8, 9));
```

## LIKE and ILIKE Operators

`LIKE` and `ILIKE` operators are used to filter results based on pattern matching. Use the `%` wildcard to match any number of characters and the `_` wildcard to match a single character.

```sql
SELECT name FROM Item WHERE name LIKE '_a%';
SELECT name FROM Item WHERE name LIKE '%r%';
```

`ILIKE` is a case-insensitive version of `LIKE`.

```sql
SELECT name FROM Item WHERE name ILIKE '%%';
SELECT name FROM Item WHERE name NOT ILIKE '%A%';
```