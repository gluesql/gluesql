---
sidebar_position: 4
---

# Transaction

`Transaction` handles BEGIN, COMMIT, and ROLLBACK. Implement its methods to support transactions.

With the defaults, statements run without a transaction and BEGIN returns an unsupported error. SAVEPOINT is not supported.

Use the shared Test Suite for commit and rollback behavior. Transaction isolation can differ between storages, so add integration tests for the guarantees your storage provides.

```rust
pub trait Transaction {
    fn begin(&mut self, autocommit: bool) -> Result<bool>;

    fn rollback(&mut self) -> Result<()>;

    fn commit(&mut self) -> Result<()>;
}
```
