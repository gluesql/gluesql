# Data Aggregation

The AST Builder API in GlueSQL allows you to construct SQL queries programmatically. This page provides an introduction to data aggregation using the AST Builder API.

## Prerequisites

Before we explore data aggregation examples, let's set up a sample table called "User" with the following columns: "id" (INT), "name" (TEXT), and "age" (INT).

```sql
CREATE TABLE User (
    id INT,
    name TEXT,
    age INT
);
```

We will use this table for the subsequent examples.

## Grouping and Counting

To group records by a specific column and count the number of occurrences in each group, you can use the AST Builder's `group_by()` and `project()` methods.

```rust
table("User")
    .select()
    .group_by("age")
    .project("age, count(*)")
    .execute(glue);
```

The above code groups the records in the "User" table by the "age" column and returns the age value along with the count of occurrences in each group. The result would be:

```
age | count(*)
----|---------
20  | 1
30  | 2
50  | 2
```

## Filtering Groups with HAVING

You can further filter the groups based on specific conditions using the `having()` method. The `having()` method allows you to apply conditions to the grouped data.

```rust
table("User")
    .select()
    .group_by("age")
    .having("count(*) > 1")
    .project("age, count(*)")
    .execute(glue);
```

The above code groups the records in the "User" table by the "age" column, but it only includes groups where the count of occurrences is greater than 1. The result would be:

```
age | count(*)
----|---------
30  | 2
50  | 2
```

This concludes the introduction to data aggregation using the AST Builder API in GlueSQL. You can leverage these methods to perform various aggregations and analyze your data effectively.
