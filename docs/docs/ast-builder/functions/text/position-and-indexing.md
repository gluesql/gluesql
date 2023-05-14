# Position and Indexing 
## Todo

GlueSQL provides several functions for text manipulation, including finding the index of a substring (`find_idx`), finding the position of a substring (`position`), and getting the leftmost or rightmost characters (`left` and `right`).

## Find Index - find_idx

`find_idx` function returns the first position of the specified substring, starting from the beginning of the string or the specified position.

```rust
let test_num = find_idx(text("strawberry"), text("berry"), None);  // Returns 6
let test_num = find_idx(text("Oracle Database 12c Release"), text("as"), Some(num(15)));  // Returns 25
```

You can also call `find_idx` directly on a string:

```rust
let test_num = text("Oracle Database 12c Release").find_idx(text("as"), Some(num(15)));  // Returns 25
```

## Position - position

The `position` function is similar to `find_idx`, but it starts counting from 1 and does not take a third argument for the starting position.

```rust
let test_num = position(text("cake"), text("ke"));  // Returns 3
```

## Left - left

The `left` function returns the leftmost characters of a string. The second argument specifies the number of characters to return.

```rust
let test_str = left(text("Hello, World"), num(7));  // Returns "Hello, "
```

## Right - right

The `right` function returns the rightmost characters of a string. The second argument specifies the number of characters to return.

```rust
let test_str = right(text("Hello, World"), num(7));  // Returns ", World"
```
