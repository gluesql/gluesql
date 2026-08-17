---
sidebar_position: 2
---

# JOIN

GlueSQL supports three types of JOIN operations:
- (INNER) JOIN
- LEFT (OUTER) JOIN
- RIGHT (OUTER) JOIN

Please note that `FULL OUTER JOIN` is currently not supported.

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

## RIGHT (OUTER) JOIN

A RIGHT JOIN (also known as RIGHT OUTER JOIN) combines rows from two tables based on a specified condition. For each row in the right table (the one named after `RIGHT JOIN`) that does not have a matching row in the left table, the result will contain NULL values for the left table's columns.

Here's an example using the provided test code:

```sql
SELECT * FROM Item RIGHT JOIN Player ON Player.id = Item.player_id;
```

This query retrieves all rows from the `Player` table and any matching rows from the `Item` table where the `id` in the `Player` table matches the `player_id` in the `Item` table. If a `Player` row has no matching `Item` row, NULL values are returned for the `Item` table columns.

With two relations, a RIGHT JOIN selects the same rows as a LEFT JOIN with the table order swapped, in a different column order:

```sql
SELECT * FROM Item RIGHT JOIN Player ON Player.id = Item.player_id;
-- selects the same rows as:
SELECT * FROM Player LEFT JOIN Item ON Player.id = Item.player_id;
```

In a longer chain the equivalence no longer holds by simply swapping two tables, because an unmatched right row is NULL-extended across *every* relation accumulated to its left:

```sql
SELECT *
FROM Player
JOIN Item ON Item.player_id = Player.id
RIGHT JOIN Trade ON Trade.item_id = Item.id;
```

A `Trade` row that matches no `Item` comes back with NULL values for both the `Player` and the `Item` columns, and it flows into any later join like any other row.

Rows are not returned in a guaranteed order, so add an `ORDER BY` when the order matters.

Remember to replace the table names, column names, and data types as needed for your specific use case.