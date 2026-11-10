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

## Automatic and explicit transactions

`begin(true)` is used for automatic transactions around the planning and execution
of one SQL statement. Its return value controls how the transaction is finished:

- Return `Ok(true)` when the call starts a transaction that should be committed
  automatically on success or rolled back on failure.
- Return `Ok(false)` when the call should not be followed by an automatic commit
  or rollback, such as when reusing an existing transaction.

An explicit `BEGIN` calls `begin(false)`. Keep that transaction open until
`COMMIT` or `ROLLBACK`. Statements within it should reuse the transaction and
return `Ok(false)` from `begin(true)`.

The default implementation returns `Ok(false)` for `begin(true)` and an
unsupported-operation error for `begin(false)`. Its `commit` and `rollback`
methods do nothing and return `Ok(())`.
