# Pattern Matching

Pattern matching is a crucial feature in SQL that allows you to match rows based on specific patterns in a column. GlueSQL provides 4 pattern matching operators: `like`, `ilike`, `not_like`, and `not_ilike`. 

Here's how you can use these operators with two special characters:

- `%`: Matches any number of characters, including zero characters
- `_`: Matches exactly one character

## LIKE Operator

The `like` operator is used in a WHERE clause to search for a specified pattern in a column. 

Here is an example:

```rust
let actual = table("Category")
    .select()
    .filter(
        col("name")
            .like(text("D%"))
            .or(col("name").like(text("M___"))),
    )
    .execute(glue)
    .await;
```

In this example, the query will return all rows from the `Category` table where the `name` column starts with "D" or where the `name` is exactly four characters long and starts with "M".

## ILIKE Operator

The `ilike` operator is used in a WHERE clause to search for a specified pattern in a column, regardless of case.

Here is an example:

```rust
let actual = table("Category")
    .select()
    .filter(
        col("name")
            .ilike(text("D%"))
            .or(col("name").ilike(text("M___"))),
    )
    .execute(glue)
    .await;
```

In this example, the query will return all rows from the `Category` table where the `name` column starts with "D" or "d", or where the `name` is exactly four characters long and starts with "M" or "m".

## NOT_LIKE Operator

The `not_like` operator is used in a WHERE clause to match rows that don't follow the specific pattern.

Here is an example:

```rust
let actual = table("Category")
    .select()
    .filter(
        col("name")
            .not_like(text("D%"))
            .and(col("name").not_like(text("M___"))),
    )
    .execute(glue)
    .await;
```

In this example, the query will return all rows from the `Category` table where the `name` column does not start with "D" and the `name` is not exactly four characters long and does not start with "M".

## NOT_ILIKE Operator

The `not_ilike` operator is used in a WHERE clause to match rows that don't follow the specific pattern, regardless of case.

Here is an example:

```rust
let actual = table("Category")
    .select()
    .filter(
        col("name")
            .not_ilike(text("D%"))
            .and(col("name").not_ilike(text("M___"))),
    )
    .execute(glue)
    .await;
```

In this example, the query will return all rows from the `Category` table where the `name` column does not start with "D" or "d", and the `name` is not exactly four characters long and does not start with "M" or "m".
