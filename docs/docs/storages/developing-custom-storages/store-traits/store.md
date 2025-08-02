---
sidebar_position: 1
---

# Store

The `Store` trait is the most essential trait to implement for custom storage. Simply by implementing the Store trait, you can support SELECT queries in SQL. You may want to analyze and retrieve data from log files or external APIs using SQL. In this case, having only SELECT queries available is sufficient, and there might not be any need for data modification. In such scenarios, implementing GlueSQL's `Store` trait alone would be adequate.

Here are the four methods required to implement the `Store` trait:

1. `fetch_schema`: This method is responsible for fetching a schema for a given table name. It returns an optional schema if the table exists.

2. `fetch_all_schemas`: This method fetches all the schemas from the storage system. It returns a vector of schemas.

3. `fetch_data`: This method fetches a specific data row from the storage system using the provided table name and key. It returns an optional data row if the key exists in the table.

4. `scan_data`: This method is used to scan all the data rows in a table. It returns an iterator over the rows in the specified table.

```rust
pub type RowIter = Box<dyn Iterator<Item = Result<(Key, DataRow)>>>;

/// By implementing `Store` trait, you can run `SELECT` query.
#[async_trait]
pub trait Store {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>>;

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>>;

    async fn scan_data(&self, table_name: &str) -> Result<RowIter>;
}
```