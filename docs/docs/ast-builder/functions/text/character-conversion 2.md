# Character Conversion

The AST Builder API in GlueSQL allows you to execute `ascii` and `chr` functions for character conversion.

## ascii

`ascii` returns the ASCII value for the specific character.

```rust
ascii<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a>
```

## chr

`chr` returns the character based on the ASCII code.

```rust
chr<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a>
```

## Examples

In these examples, the ASCII and CHR functions should return matching values.

```rust
use {
    gluesql_core::{ast_builder::{function as f, *}}
};

values(vec![
    vec![f::ascii("'\t'"), f::chr(9)],
    vec![f::ascii("'\n'"), f::chr(10)],
    vec![f::ascii("'\r'"), f::chr(13)],
    vec![f::ascii("' '"), f::chr(32)],
    vec![f::ascii("'!'"), f::chr(33)],
    vec![f::ascii("'\"'"), f::chr(34)],
    vec![f::ascii("'#'"), f::chr(35)],
    vec![f::ascii("'$'"), f::chr(36)],
    vec![f::ascii("'%'"), f::chr(37)],
    vec![f::ascii("'&'"), f::chr(38)],
    vec![f::ascii("''''"), f::chr(39)],
    vec![f::ascii("','"), f::chr(44)],
])
.alias_as("Sub")
.select()
.project("column1 AS ascii")
.project("column2 AS char")
.execute(glue)
.await;
```

| ascii | char |
| ----- | ---- |
| 9     | \t   |
| 10    | \n   |
| 13    | \r   |
| 32    | " "  |
| 33    | !    |
| 34    | "    |
| 35    | #    |
| 36    | $    |
| 37    | %    |
| 38    | &    |
| 39    | '    |
| 44    | ,    |
