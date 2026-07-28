---
title: "Window Functions"
sidebar_position: 5
---

# Window Functions

A window function computes a value for every row of the result by looking at a set of related rows, chosen with an `OVER` clause. Unlike `GROUP BY`, it does not collapse those rows, so each input row still appears in the output.

## Supported functions

Ranking functions:

- `ROW_NUMBER()`: Numbers the rows of each partition consecutively starting at one.
- `RANK()`: Gives every peer group the number of its first row, leaving gaps after ties.
- `DENSE_RANK()`: Numbers peer groups consecutively without gaps.

Offset functions:

- `LAG(expr [, offset [, default]])`: Evaluates `expr` at the row before the current row.
- `LEAD(expr [, offset [, default]])`: Evaluates `expr` at the row after the current row.

Aggregates used as window functions:

- `COUNT`, `SUM`, `MIN`, `MAX` and `AVG`.

## The OVER clause

`OVER` accepts an optional `PARTITION BY` list and an optional `ORDER BY` list.

```sql
SELECT
    region,
    amount,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rank_in_region
FROM Sale;
```

`PARTITION BY` splits the rows into independent groups. Rows whose partition expressions are all equal belong to the same partition, and rows with `NULL` partition values group together the same way `GROUP BY` groups them. Omitting `PARTITION BY` treats the whole result as one partition.

`ORDER BY` inside `OVER` orders the rows of each partition and follows the same `ASC` and `DESC` behavior as query-level `ORDER BY`. Rows that tie on every ordering expression are peers. With no `ORDER BY`, every row of the partition is a peer of every other.

## Window aggregates

An aggregate with no `ORDER BY` in its `OVER` clause computes over the whole partition:

```sql
SELECT region, amount, SUM(amount) OVER (PARTITION BY region) AS region_total FROM Sale;
```

With `ORDER BY` it computes a running value. For each row it covers the partition from its first row through the current row and all the current row's peers, so peers always share the same result:

```sql
SELECT region, amount, SUM(amount) OVER (PARTITION BY region ORDER BY id) AS running_total FROM Sale;
```

`NULL` values are handled exactly as the ordinary aggregates handle them.

## Offset functions

`LAG` and `LEAD` take an optional second argument giving the distance, which defaults to one, and an optional third argument giving the value to return when the target row falls outside the partition. Without it the result is `NULL` for such rows.

```sql
SELECT id, amount, LAG(amount, 1, 0) OVER (ORDER BY id) AS previous FROM Sale;
```

## Restrictions

Window functions are allowed in the `SELECT` projection, including inside larger expressions and under column aliases. Using one anywhere else is an error.

The following are also errors:

- Combining a window function with `GROUP BY`, `HAVING` or `SELECT DISTINCT` in the same query.
- Nesting a window function inside another window function or inside a regular aggregate.
- A `LAG` or `LEAD` offset that is negative, fractional, or not a literal.
- `DISTINCT` inside a window aggregate.
- An explicit frame clause such as `ROWS` or `RANGE` inside `OVER`.
