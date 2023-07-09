---
sidebar_position: 5
---

# CustomFunction

The `CustomFunction` trait is an optional trait for supporting user-level custom functions. Through the `CustomFunction` trait, you can retrieve custom functions stored in the storage system. You can choose to implement the `CustomFunction` trait alone or together with the `CustomFunctionMut` trait.

In some cases, you might want to provide storage-specific functions pre-built and separately available for each storage system. In such cases, you can implement the `CustomFunction` trait and create additional functions stored in advance when using it. To achieve this, the `CustomFunction` and `CustomFunctionMut` traits are provided separately for implementation.

There are two methods available:

1. `fetch_function`: This method retrieves a custom function from the storage system using the provided function name.

2. `fetch_all_functions`: This method retrieves all custom functions stored in the storage system.

```rust
#[async_trait(?Send)]
pub trait CustomFunction {
    async fn fetch_function(&self, _func_name: &str) -> Result<Option<&StructCustomFunction>>;

    async fn fetch_all_functions(&self) -> Result<Vec<&StructCustomFunction>>;
}
```
