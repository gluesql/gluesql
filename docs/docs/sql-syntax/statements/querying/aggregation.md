---
sidebar_position: 4
---

# Aggregation

GlueSQL supports several aggregate functions to perform calculations on a set of values. Below is a list of supported aggregate functions along with a brief explanation of each:

- `COUNT`: Counts the number of non-NULL values in the specified column.
- `AVG`: Calculates the average of non-NULL values in the specified column.
- `SUM`: Calculates the sum of non-NULL values in the specified column.
- `MAX`: Returns the maximum value in the specified column.
- `MIN`: Returns the minimum value in the specified column.
- `STDEV`: Calculates the population standard deviation of non-NULL values in the specified column.
- `VARIANCE`: Calculates the population variance of non-NULL values in the specified column.

In addition to the aggregate functions, you can use `GROUP BY` and `HAVING` clauses to group and filter the results based on specific conditions.

## GROUP BY

The `GROUP BY` clause is used to group rows with the same values in specified columns into a set of summary rows. It is often used with aggregate functions to perform calculations on each group of rows.

Here's an example that groups the items by `city` and calculates the sum of `quantity` and the count of items for each city:

```sql
SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city;
```

## HAVING

The `HAVING` clause is used to filter the results of a `GROUP BY` query based on a condition that applies to the summary rows. It is similar to the `WHERE` clause but operates on the results of the grouping.

Here's an example that groups the items by `city` and calculates the sum of `quantity` and the count of items for each city, but only includes cities with a count greater than 1:

```sql
SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city HAVING COUNT(*) > 1;
```

In the examples provided, you can see the usage of `GROUP BY` and `HAVING` clauses in combination with aggregate functions to retrieve data from the `Item` table.