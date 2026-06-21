# Rounding 

The AST (Abstract Syntax Tree) Builder in GlueSQL provides several mathematical functions, including `round`, `ceil`, and `floor`. These functions are used to perform rounding operations on floating-point numbers.

For the sake of this tutorial, we'll assume there's a table named `Number` with columns `id` (of type `INTEGER`) and `number` (of type `FLOAT`).

## Ceil Function

The `ceil` function rounds up the `number` to the nearest integer value that is greater than or equal to `number`.

In GlueSQL, you can call this function in two ways. Both methods are shown below:

```rust
let actual = table("Number")
    .select()
    .project("id")
    .project(ceil("number"))  // Method 1: Using the ceil function directly
    .project(col("number").ceil())  // Method 2: Calling the ceil method on a column
    .execute(glue)
    .await;
```

## Floor Function

The `floor` function rounds down the `number` to the nearest integer value that is less than or equal to `number`.

Again, there are two ways to call this function in GlueSQL:

```rust
let actual = table("Number")
    .select()
    .project("id")
    .project(floor("number"))  // Method 1: Using the floor function directly
    .project(col("number").floor())  // Method 2: Calling the floor method on a column
    .execute(glue)
    .await;
```

## Round Function

The `round` function rounds the `number` to the nearest integer. If `number` is halfway between two integers, it rounds towards the nearest even number.

The `round` function can also be called in two ways, as demonstrated below:

```rust
let actual = table("Number")
    .select()
    .project("id")
    .project(round("number"))  // Method 1: Using the round function directly
    .project(col("number").round())  // Method 2: Calling the round method on a column
    .execute(glue)
    .await;
```