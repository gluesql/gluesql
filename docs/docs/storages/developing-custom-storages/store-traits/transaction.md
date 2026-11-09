---
sidebar_position: 4
---

# Transaction

While transactions are often considered an essential feature for databases, GlueSQL treats transactions as an optional trait. Custom storage developers can choose whether or not to support transactions in their storage implementation. Transactions can be quite heavy and expensive in terms of performance.

If you're building a general-purpose OLTP database, transactions are a necessary feature. However, if you want to handle JSONL log files using SQL, transactions may be desirable, but not strictly necessary at the cost of significant performance degradation.

You can verify your `Transaction` trait implementation using the Test Suite. However, the Test Suite only provides logical tests for single-threaded environments. If you intend to support transactions in a concurrent environment, you'll need to write additional tests to verify your implementation. This allows different storage implementations to support various transaction isolation levels.

Currently, the SAVEPOINT feature is not supported, and only three methods are available: BEGIN (or START TRANSACTION), ROLLBACK, and COMMIT.

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
