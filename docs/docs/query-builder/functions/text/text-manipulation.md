# Text Manipulation

## Functions

- CONCAT: Concatenates two or more strings into one.
- CONCAT_WS: Concatenates two or more strings into one with a separator.
- SUBSTR: Returns a part of a string.
- REPEAT: Repeats a string a specified number of times.
- REVERSE: Reverses the order of the characters in a string.
- REPLACE: Replaces occurrences of one substring with another.
- HEX: Converts an integer or string value to hexadecimal text.

## REPLACE - replace

The `replace` function returns a string with all occurrences of one substring replaced by another substring.

```rust
let actual = table("Item")
    .select()
    .project(col("name").replace(text("T"), text("S")))
    .execute(glue);
```

You can also call it as a function:

```rust
let actual = values(vec![
    vec![query_builder::function::replace(text("Tticky GlueTQL"), text("T"), text("S"))],
])
.execute(glue);
```

## HEX - hex

The `hex` function converts an integer or string value to uppercase hexadecimal text.

```rust
let actual = values(vec![
    vec![query_builder::function::hex(text("Hello World"))],
    vec![query_builder::function::hex(num(228))],
])
.execute(glue);
```
