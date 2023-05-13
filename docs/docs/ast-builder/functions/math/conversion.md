# Conversion 

The AST (Abstract Syntax Tree) Builder in GlueSQL provides mathematical conversion functions like `degrees` and `radians`. These functions convert angles expressed in radians to degrees and vice versa.

For the sake of this tutorial, we'll assume there's a table named `Number` with columns `input` (of type `INTEGER`) and `number` (of type `FLOAT`).

## Degrees Function

The `degrees` function converts an angle from radians to degrees.

You can call this function in two ways in GlueSQL:

```rust
let actual = table("Number")
    .select()
    .project("input")
    .project(degrees("number"))  // Method 1: Using the degrees function directly
    .project(col("number").degrees())  // Method 2: Calling the degrees method on a column
    .execute(glue)
    .await;
```

## Radians Function

The `radians` function converts an angle from degrees to radians.

Just like with the `degrees` function, there are two ways to call this function:

```rust
let actual = table("Number")
    .select()
    .project("input")
    .project(radians("number"))  // Method 1: Using the radians function directly
    .project(col("number").radians())  // Method 2: Calling the radians method on a column
    .execute(glue)
    .await;
```