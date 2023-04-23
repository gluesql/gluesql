---
sidebar_position: 2
---

# JOIN

GlueSQL supports two types of JOIN operations:
- (INNER) JOIN
- LEFT (OUTER) JOIN

Please note that `FULL OUTER JOIN` and `RIGHT JOIN` are currently not supported.

## (INNER) JOIN

An INNER JOIN combines rows from two tables based on a specified condition. Rows that do not satisfy the condition are excluded from the result.

Here's an example using the provided test code:

```sql
SELECT * FROM Item INNER JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1;
```

This query retrieves all rows from the `Item` and `Player` tables where the `id` in the `Player` table matches the `player_id` in the `Item` table, and the `Player.id` is equal to 1.

## LEFT (OUTER) JOIN

A LEFT JOIN (also known as LEFT OUTER JOIN) combines rows from two tables based on a specified condition. For each row in the left table that does not have a matching row in the right table, the result will contain NULL values.

Here's an example using the provided test code:

```sql
SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE quantity = 1;
```

This query retrieves all rows from the `Item` table and any matching rows from the `Player` table where the `id` in the `Player` table matches the `player_id` in the `Item` table. If there's no match, NULL values are returned for the `Player` table columns. The result is then filtered by the `quantity` column in the `Item` table with a value of 1.

Remember to replace the table names, column names, and data types as needed for your specific use case.