---
sidebar_position: 5
---

# CustomFunction

`CustomFunction` reads user-defined functions from storage. Override its methods to expose functions; their defaults return unsupported errors.

For example, a storage can provide a fixed set of functions through `CustomFunction`. Use `CustomFunctionMut` to support creating and deleting functions.

It provides two methods:

1. `fetch_function`: This method retrieves a custom function from the storage system using the provided function name.

2. `fetch_all_functions`: This method retrieves all custom functions stored in the storage system.

```rust
pub trait CustomFunction {
    fn fetch_function<'a>(&'a self, func_name: &str) -> Result<Option<&'a StructCustomFunction>>;

    fn fetch_all_functions(&self) -> Result<Vec<&StructCustomFunction>>;
}
```
