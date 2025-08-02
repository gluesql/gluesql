---
sidebar_position: 4
---

# Transaction

While transactions are often considered an essential feature for databases, GlueSQL treats transactions as an optional trait. Custom storage developers can choose whether or not to support transactions in their storage implementation. Transactions can be quite heavy and expensive in terms of performance.

If you're building a general-purpose OLTP database, transactions are a necessary feature. However, if you want to handle JSONL log files using SQL, transactions may be desirable, but not strictly necessary at the cost of significant performance degradation.

You can verify your `Transaction` trait implementation using the Test Suite. However, the Test Suite only provides logical tests for single-threaded environments. If you intend to support transactions in a concurrent environment, you'll need to write additional tests to verify your implementation. This allows different storage implementations to support various transaction isolation levels.

Currently, the SAVEPOINT feature is not supported, and only three methods are available: BEGIN (or START TRANSACTION), ROLLBACK, and COMMIT.

```rust
#[async_trait]
pub trait Transaction {
    async fn begin(&mut self, autocommit: bool) -> Result<bool>;

    async fn rollback(&mut self) -> Result<()>;

    async fn commit(&mut self) -> Result<()>;
}
```