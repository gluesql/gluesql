---
sidebar_position: 6
---

# CustomFunctionMut

By implementing both the `CustomFunction` and `CustomFunctionMut` traits, users can create, use, and delete user-level custom functions. Although GlueSQL plans to continuously add various functions, users may still find them insufficient. In such cases, users can create their own user-level custom functions to supplement the built-in functions. Additionally, if there are repetitive business logic codes, they can be stored as custom functions.

Example:

```sql
CREATE FUNCTION ADD_ONE (n INT, x INT DEFAULT 1) RETURN n + x;

SELECT ADD_ONE(10) AS test;

DROP FUNCTION ADD_ONE;
```

There are two methods available:

1. `insert_function`: This method inserts a new custom function into the storage system.

2. `delete_function`: This method deletes a custom function from the storage system using the provided function name.

```rust
#[async_trait]
pub trait CustomFunctionMut {
    async fn insert_function(&mut self, _func: StructCustomFunction) -> Result<()>;

    async fn delete_function(&mut self, _func_name: &str) -> Result<()>;
}
```