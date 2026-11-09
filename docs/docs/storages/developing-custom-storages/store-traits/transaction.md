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

## Execution and transaction ownership

For each ordinary statement, `Glue::execute` calls `begin(true)` before planning
and execution. `Glue::execute_stmt` does the same for execution of an already
prepared plan. Query Builder execution uses the same Glue path as SQL execution.
When `begin(true)` returns `true`, Glue commits on success and rolls back on
failure. When it returns `false`, Glue returns the operation's result without
calling commit or rollback. The default trait implementation returns `Ok(false)`
for `begin(true)`, so storage implementations without transaction support keep
working through this path.

SQL `BEGIN`, `COMMIT`, and `ROLLBACK` bypass automatic transaction handling and
invoke the corresponding storage operation. The default implementation rejects
an explicit `BEGIN`, which calls `begin(false)`.

The low-level `executor::execute`, also exported as `prelude::execute`, runs a
prepared plan without automatic transaction handling. This changes its previous
behavior: direct callers must manage the surrounding transaction when required
by their storage, or use `Glue::execute_stmt` to have Glue manage it. Planning
remains separate from this low-level execution API.
