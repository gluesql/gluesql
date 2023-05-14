# Basic Arithmetic 

GlueSQL provides a number of basic arithmetic operations such as absolute value (abs), division (divide), modulo (modulo), greatest common divisor (gcd), and least common multiple (lcm).

For this tutorial, we assume there's a table named `Number` with columns `id` and `number`.

## Absolute Value - ABS

The `abs` function returns the absolute value of a number.

```rust
let actual = values(vec!["0, 0", "1, -3", "2, 4", "3, -29"])
    .alias_as("number")
    .select()
    .project("column1")
    .project(abs("column2"))  // Takes the absolute value of column2
    .project(col("column2").abs())  // Takes the absolute value of column2
    .execute(glue)
    .await;
```

## Division - DIV

The `divide` function divides one number by another.

```rust
let actual = table("Number")
    .select()
    .project("id")
    .project(divide("number", 3))  // Divides the number by 3
    .project(divide(col("number"), 3))  // Divides the number by 3
    .execute(glue)
    .await;
```

## Modulo - MOD

The `modulo` function returns the remainder of one number divided by another.

```rust
let actual = table("Number")
    .select()
    .project("id")
    .project(modulo("number", 4))  // Gets the remainder of number divided by 4
    .project(modulo(col("number"), 4))  // Gets the remainder of number divided by 4
    .execute(glue)
    .await;
```

## Greatest Common Divisor - GCD

The `gcd` function returns the greatest common divisor of two numbers.

```rust
let actual = table("Number")
    .select()
    .project("id")
    .project(gcd("number", 12))  // Gets the GCD of number and 12
    .project(gcd(col("number"), 12))  // Gets the GCD of number and 12
    .execute(glue)
    .await;
```

## Least Common Multiple - LCM

The `lcm` function returns the least common multiple of two numbers.

```rust
let actual = table("Number")
    .select()
    .project("id")
    .project(lcm("number", 3))  // Gets the LCM of number and 3
    .project(lcm(col("number"), 3))  // Gets the LCM of number and 3
    .execute(glue)
    .await;
```
